package handlers

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/celestix/gotgproto/dispatcher"
	"github.com/celestix/gotgproto/ext"
	"github.com/celestix/gotgproto/functions"
	"github.com/gotd/td/tg"
	"github.com/rs/xid"

	"github.com/krau/SaveAny-Bot/client/bot/handlers/utils/mediautil"
	"github.com/krau/SaveAny-Bot/client/bot/handlers/utils/ruleutil"
	userclient "github.com/krau/SaveAny-Bot/client/user"
	"github.com/krau/SaveAny-Bot/common/i18n"
	"github.com/krau/SaveAny-Bot/common/i18n/i18nk"
	"github.com/krau/SaveAny-Bot/common/progressmsg"
	"github.com/krau/SaveAny-Bot/common/utils/tgutil"
	"github.com/krau/SaveAny-Bot/config"
	"github.com/krau/SaveAny-Bot/core"
	"github.com/krau/SaveAny-Bot/core/tasks/collect"
	tftask "github.com/krau/SaveAny-Bot/core/tasks/tfile"
	"github.com/krau/SaveAny-Bot/database"
	"github.com/krau/SaveAny-Bot/pkg/tfile"
	"github.com/krau/SaveAny-Bot/storage"
)

var collectPaths collect.PathLocks

// /collect is deliberately non-interactive, including when silent is disabled.
func handleCollectCmd(ctx *ext.Context, update *ext.Update) error {
	reply := func(key i18nk.Key, data map[string]any) error {
		_, err := ctx.Reply(update, ext.ReplyTextString(i18n.T(key, data)), nil)
		if err != nil {
			return err
		}
		return dispatcher.EndGroups
	}
	opts, err := collect.Parse(update.EffectiveMessage.Text)
	if err != nil {
		return reply(i18nk.CollectUsage, nil)
	}
	uc := userclient.GetCtx()
	if !config.C().Telegram.Userbot.Enable || uc == nil {
		return reply(i18nk.CollectNeedUserbot, nil)
	}
	user, err := database.GetUserByChatID(ctx, update.GetUserChat().GetID())
	if err != nil {
		return reply(i18nk.CollectError, map[string]any{"Error": err.Error()})
	}
	storName := opts.Storage
	if storName == "" {
		storName = user.DefaultStorage
	}
	if storName == "" {
		return reply(i18nk.BotMsgCommonErrorDefaultStorageNotSet, nil)
	}
	stor, err := storage.GetStorageByUserIDAndName(ctx, user.ChatID, storName)
	if err != nil {
		return reply(i18nk.CollectError, map[string]any{"Error": err.Error()})
	}
	if _, ok := stor.(storage.StorageExistenceChecker); !ok {
		return reply(i18nk.CollectUnsupportedStorage, map[string]any{"Storage": storName})
	}
	dir := ""
	if user.DefaultDir != 0 {
		d, err := database.GetDirByID(ctx, user.DefaultDir)
		if err != nil {
			return reply(i18nk.CollectError, map[string]any{"Error": err.Error()})
		}
		if d.UserID == user.ID && d.StorageName == storName {
			dir = d.Path
		}
	}
	id := xid.New().String()
	m, err := ctx.Reply(update, ext.ReplyTextString(i18n.T(i18nk.CollectQueued, map[string]any{"ID": id})), &ext.ReplyOpts{Markup: collectCancelMarkup(id)})
	if err != nil {
		return err
	}
	taskCtx := tgutil.ExtWithContext(ctx.Context, ctx)
	editor := progressmsg.New(taskCtx, user.ChatID, config.ProgressInterval())
	p := &collectProgress{editor: editor, id: id, messageID: m.ID}
	task := &collect.Task{ID: id, Tag: opts.Tag, Concurrency: max(1, config.C().Workers)}
	task.Fetch = collectHistory(uc, opts.Chat)
	task.Process = func(c context.Context, msg *tg.Message, album []*tg.Message) (bool, error) {
		slots := collectSlots()
		select {
		case slots <- struct{}{}:
		case <-c.Done():
			return false, c.Err()
		}
		defer func() { <-slots }()
		progress := p.add(strconv.Itoa(msg.ID))
		defer progress.close()
		return collectFile(c, id, user, uc, stor, dir, msg, album, progress)
	}
	task.Report = p.report

	if err := core.AddTask(taskCtx, task); err != nil {
		task.Report(collect.Stats{}, true, err)
	}
	return dispatcher.EndGroups
}

// The descending history cursor fixes the upper bound at the first page and
// excludes newly arriving messages. Full history scanning avoids search-index
// omissions and retains captions on photo members of mixed video albums.
func collectHistory(uc *ext.Context, chat string) func(context.Context, int) (collect.Page, error) {
	var peer tg.InputPeerClass
	return func(ctx context.Context, offset int) (collect.Page, error) {
		var page collect.Page
		c := *uc
		c.Context = ctx
		if peer == nil {
			if strings.HasPrefix(chat, "https://t.me/") || strings.HasPrefix(chat, "http://t.me/") {
				u, err := url.Parse(chat)
				if err != nil {
					return page, err
				}
				parts := strings.Split(strings.Trim(u.Path, "/"), "/")
				if len(parts) == 2 && parts[0] == "c" {
					chat = "-100" + parts[1]
				} else if len(parts) == 1 {
					chat = parts[0]
				} else {
					return page, fmt.Errorf("provide a chat username or ID, not a message/invite link")
				}
			}
			id, err := tgutil.ParseChatID(&c, chat)
			if err != nil {
				return page, fmt.Errorf("resolve chat: %w", err)
			}
			peer, err = c.ResolveInputPeerById(id)
			if err != nil {
				return page, fmt.Errorf("resolve peer: %w", err)
			}
			switch peer.(type) {
			case *tg.InputPeerChannel, *tg.InputPeerChat:
			default:
				return page, fmt.Errorf("collect requires a group or channel")
			}
		}
		// Pace page requests even when every item is skipped. Telegram's existing
		// FLOOD_WAIT middleware handles server-specified waits and cancellation.
		if offset > 0 {
			if err := collectWait(ctx, time.Second); err != nil {
				return page, err
			}
		}
		result, err := c.Raw.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, OffsetID: offset, Limit: collect.PageSize})
		if err != nil {
			return page, err
		}
		messages, ok := result.(interface{ GetMessages() []tg.MessageClass })
		if !ok {
			return page, fmt.Errorf("unexpected history result %T", result)
		}
		items := messages.GetMessages()
		page.Done = len(items) == 0
		for _, item := range items {
			id := item.GetID()
			if id > 0 && (page.Next == 0 || id < page.Next) {
				page.Next = id
			}
			if m, ok := item.(*tg.Message); ok {
				page.Messages = append(page.Messages, m)
			}
		}
		return page, nil
	}
}

func collectWait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func collectProbe(ctx context.Context, s storage.Storage, p string) (bool, error) {
	var err error
	for i := 0; i < 3; i++ {
		var exists bool
		exists, err = storage.CheckExists(ctx, s, p)
		if err == nil {
			return exists, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if i < 2 {
			if e := collectWait(ctx, time.Duration(i+1)*time.Second); e != nil {
				return false, e
			}
		}
	}
	return false, fmt.Errorf("destination check: %w", err)
}

// A second probe runs at the upload boundary, after the existing task has
// downloaded the file. Returning nil on a duplicate still runs its cleanup.
type collectStorage struct {
	storage.Storage
	skipped bool
}

func (s *collectStorage) CannotStream() string { return "collect checks destination before uploading" }
func (s *collectStorage) Save(ctx context.Context, r io.Reader, p string) error {
	exists, err := collectProbe(ctx, s.Storage, p)
	if err != nil {
		return err
	}
	if exists {
		s.skipped = true
		return nil
	}
	return s.Storage.Save(ctx, r, p)
}

func collectFile(ctx context.Context, id string, user *database.User, uc *ext.Context, base storage.Storage, dir string, msg *tg.Message, album []*tg.Message, trackers ...tftask.ProgressTracker) (bool, error) {
	file, err := tfile.FromMediaMessage(msg.Media, uc.Raw, msg, mediautil.TfileOptions(ctx, user, msg)...)
	if err != nil {
		return false, err
	}
	// Names must be stable and safe before probing or allocating local files.
	name := strings.ReplaceAll(file.Name(), "\\", "_")
	name = path.Base(name)
	if name == "" || name == "." || name == "/" || name == ".." {
		name = "video_" + strconv.Itoa(msg.ID)
	}
	if path.Ext(name) == "" {
		ext := ".mp4"
		if m, ok := msg.Media.(*tg.MessageMediaDocument); ok {
			if d, ok := m.Document.AsNotEmpty(); ok {
				if exts, _ := mime.ExtensionsByType(d.MimeType); len(exts) > 0 {
					ext = exts[0]
				}
			}
		}
		name += ext
	}
	file.SetName(name)
	stor := base
	if user.ApplyRule {
		matched, s, p := ruleutil.ApplyRule(ctx, user.Rules, ruleutil.NewInput(file))
		if matched {
			if s.Usable() {
				stor, err = storage.GetStorageByUserIDAndName(ctx, user.ChatID, s.String())
				if err != nil {
					return false, err
				}
			}
			if p.NeedNewForAlbum() {
				if msg.GroupedID != 0 {
					// Use the oldest video filename, matching normal album folder
					// naming while remaining deterministic across repeated scans.
					albumName := name
					for i := len(album) - 1; i >= 0; i-- {
						if !collect.IsVideo(album[i]) {
							continue
						}
						first, e := tfile.FromMediaMessage(album[i].Media, uc.Raw, album[i], mediautil.TfileOptions(ctx, user, album[i])...)
						if e != nil {
							return false, e
						}
						albumName = path.Base(strings.ReplaceAll(first.Name(), "\\", "_"))
						break
					}
					albumName = strings.TrimSuffix(albumName, path.Ext(albumName))
					if albumName == "" || albumName == "." || albumName == ".." || albumName == "/" {
						albumName = fmt.Sprintf("album_%d", msg.GroupedID)
					}
					dir = path.Join(dir, albumName)
				}
			} else if p != "" {
				dir = p.String()
			}
		}
	}
	if len(trackers) > 0 {
		if progress, ok := trackers[0].(*collectItemProgress); ok {
			progress.update(func(s *collectItemState) { s.name = name; s.total = file.Size() })
		}
	}
	p := path.Join(dir, name)
	unlock, err := collectPaths.Acquire(ctx, stor.Name()+"\x00"+p)
	if err != nil {
		return false, err
	}
	defer unlock()
	exists, err := collectProbe(ctx, stor, p)
	if err != nil || exists {
		return exists, err
	}
	// Refresh short-lived Telegram file references only for files that actually
	// need downloading; a page can take hours to process on a small VPS.
	freshCtx := *uc
	freshCtx.Context = ctx
	messages, err := freshCtx.GetMessages(functions.GetChatIdFromPeer(msg.PeerID), []tg.InputMessageClass{&tg.InputMessageID{ID: msg.ID}})
	if err != nil {
		return false, fmt.Errorf("refresh message: %w", err)
	}
	if len(messages) != 1 {
		return false, fmt.Errorf("source message unavailable")
	}
	fresh, ok := messages[0].(*tg.Message)
	if !ok || !collect.IsVideo(fresh) {
		return false, fmt.Errorf("source video unavailable")
	}
	file, err = tfile.FromMediaMessage(fresh.Media, uc.Raw, fresh, tfile.WithName(name))
	if err != nil {
		return false, fmt.Errorf("refresh file: %w", err)
	}
	var tracker tftask.ProgressTracker
	if len(trackers) > 0 {
		tracker = trackers[0]
	}
	guard := &collectStorage{Storage: stor}
	task, err := tftask.NewTGFileTask(id+"_"+strconv.Itoa(msg.ID), ctx, file, guard, p, tracker)
	if err != nil {
		return false, err
	}
	if err = task.Execute(ctx); err != nil {
		return false, err
	}
	return guard.skipped, nil
}

func collectCancelMarkup(id string) tg.ReplyMarkupClass {
	return &tg.ReplyInlineMarkup{Rows: []tg.KeyboardButtonRow{{Buttons: []tg.KeyboardButtonClass{tgutil.BuildCancelButton(id)}}}}
}
