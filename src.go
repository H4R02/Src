package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const sessionsDir = "sessions"

// Environment Variables
var (
	ApiID    int32 // Fixed: Set strictly as int32 to match telegram.ClientConfig
	ApiHash  string
	BotToken string
	MongoURI string
	OwnerID  int64
)

// Load Environment Variables
func loadEnvVariables() {
	var err error

	apiIDInt, err := strconv.Atoi(os.Getenv("API_ID"))
	if err != nil {
		fmt.Println("❌ Error: API_ID must be a valid integer.")
		os.Exit(1)
	}
	ApiID = int32(apiIDInt) // Cast int to int32

	ApiHash = os.Getenv("API_HASH")
	BotToken = os.Getenv("BOT_TOKEN")
	MongoURI = os.Getenv("MONGO_URI")

	OwnerID, err = strconv.ParseInt(os.Getenv("OWNER_ID"), 10, 64)
	if err != nil {
		fmt.Println("❌ Error: OWNER_ID must be a valid integer.")
		os.Exit(1)
	}

	if ApiHash == "" || BotToken == "" || MongoURI == "" {
		fmt.Println("❌ Error: Missing required Environment Variables (API_HASH, BOT_TOKEN, MONGO_URI).")
		os.Exit(1)
	}
	fmt.Println("✅ Environment Variables Loaded Successfully!")
}

// Pre-compiled regex patterns for performance optimization
var (
	tmeLinkRe     = regexp.MustCompile(`t\.me/(c/)?([A-Za-z0-9_]+)/(?:\d+/)?(\d+)`)
	phoneRe       = regexp.MustCompile(`^\+?[0-9]{7,15}$`)
	usernameRe    = regexp.MustCompile(`(?i)@[a-z0-9_]+`)
	urlRe         = regexp.MustCompile(`(?i)https?://(t\.me|telegram\.me)[^\s]+`)
	splitRe       = regexp.MustCompile(`[\n,]+`)
	illegalCharRe = regexp.MustCompile(`[<>:"/\\|?*]`)
)

// HTTP Client with 15-second timeout to prevent goroutine hangs
var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

var (
	mongoClient      *mongo.Client
	db               *mongo.Database
	sessionsCol      *mongo.Collection
	topicMapCol      *mongo.Collection
	batchStateCol    *mongo.Collection
	cloneProgressCol *mongo.Collection
	adminsCol        *mongo.Collection
	botStatsCol      *mongo.Collection
)

type BotStats struct {
	ID         string    `bson:"_id"`
	TotalFiles int       `bson:"total_files"`
	TotalBytes int64     `bson:"total_bytes"`
	StartTime  time.Time `bson:"start_time"`
}

func initMongoDB() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(MongoURI)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		fmt.Println("❌ MongoDB Connection Error:", err)
		os.Exit(1)
	}

	mongoClient = client
	db = mongoClient.Database("tg_cloner_bot")
	sessionsCol = db.Collection("sessions")
	topicMapCol = db.Collection("topic_name_mappings")
	batchStateCol = db.Collection("batch_states")
	cloneProgressCol = db.Collection("clone_progress")
	adminsCol = db.Collection("admins")
	botStatsCol = db.Collection("bot_stats")

	count, _ := botStatsCol.CountDocuments(ctx, bson.M{"_id": "global_stats"})
	if count == 0 {
		botStatsCol.InsertOne(ctx, BotStats{ID: "global_stats", StartTime: time.Now()})
	}

	fmt.Println("✅ MongoDB Connected Successfully!")
}

func incBotStats(fileSize int64) {
	botStatsCol.UpdateOne(
		context.Background(),
		bson.M{"_id": "global_stats"},
		bson.M{"$inc": bson.M{"total_files": 1, "total_bytes": fileSize}},
	)
}

type UserSession struct {
	Phone         string `bson:"phone"`
	SessionString string `bson:"session_string"`
	OwnerChatID   int64  `bson:"owner_chat_id"`
}

func saveSessionToDB(phone string, sessionStr string, chatID int64) {
	filter := bson.M{"phone": phone}
	update := bson.M{"$set": bson.M{
		"phone":          phone,
		"session_string": sessionStr,
		"owner_chat_id":  chatID,
	}}
	opts := options.Update().SetUpsert(true)
	sessionsCol.UpdateOne(context.Background(), filter, update, opts)
}

func getSessionFromDB(phone string) string {
	var s UserSession
	err := sessionsCol.FindOne(context.Background(), bson.M{"phone": phone}).Decode(&s)
	if err != nil {
		return ""
	}
	return s.SessionString
}

func deleteSessionFromDB(phone string) {
	sessionsCol.DeleteOne(context.Background(), bson.M{"phone": phone})
}

type AdminRecord struct {
	UserID  int64     `bson:"user_id"`
	AddedBy int64     `bson:"added_by"`
	AddedAt time.Time `bson:"added_at"`
}

func addAdmin(userID, addedBy int64) {
	filter := bson.M{"user_id": userID}
	update := bson.M{"$set": bson.M{
		"user_id":  userID,
		"added_by": addedBy,
		"added_at": time.Now(),
	}}
	opts := options.Update().SetUpsert(true)
	adminsCol.UpdateOne(context.Background(), filter, update, opts)
}

func removeAdmin(userID int64) {
	adminsCol.DeleteOne(context.Background(), bson.M{"user_id": userID})
}

func isAdmin(userID int64) bool {
	count, err := adminsCol.CountDocuments(context.Background(), bson.M{"user_id": userID})
	if err != nil {
		return false
	}
	return count > 0
}

func isOwner(userID int64) bool {
	return userID == OwnerID
}

func isAuthorized(userID int64) bool {
	return isOwner(userID) || isAdmin(userID)
}

func sendAdminList(client *telegram.Client, chatID int64) {
	cur, err := adminsCol.Find(context.Background(), bson.M{})
	if err != nil {
		client.SendMessage(chatID, "❌ Failed to fetch admin list.", &telegram.SendOptions{ParseMode: "HTML"})
		return
	}
	defer cur.Close(context.Background())

	var lines []string
	for cur.Next(context.Background()) {
		var a AdminRecord
		if decodeErr := cur.Decode(&a); decodeErr == nil {
			lines = append(lines, fmt.Sprintf("• <code>%d</code>", a.UserID))
		}
	}
	if len(lines) == 0 {
		client.SendMessage(chatID, "ℹ️ No admins added yet. Owner always has full access.", &telegram.SendOptions{ParseMode: "HTML"})
		return
	}
	client.SendMessage(chatID, "👥 <b>Bot Admins:</b>\n"+strings.Join(lines, "\n"), &telegram.SendOptions{ParseMode: "HTML"})
}

type ReplacePair struct {
	Old string `bson:"old"`
	New string `bson:"new"`
}

type PersistedBatch struct {
	ChatID        int64         `bson:"chat_id"`
	StartPeer     string        `bson:"start_peer"`
	StartID       int           `bson:"start_id"`
	EndID         int           `bson:"end_id"`
	LastSuccess   int           `bson:"last_success"`
	TargetChat    string        `bson:"target_chat"`
	MediaFilter   string        `bson:"media_filter"`
	Prefix        string        `bson:"prefix"`
	Suffix        string        `bson:"suffix"`
	ReplacePairs  []ReplacePair `bson:"replace_pairs"`
	CaptionRemove string        `bson:"caption_remove"`
	FileRemove    string        `bson:"file_remove"`
	IsClone       bool          `bson:"is_clone"`
}

func saveBatchState(chatID int64, ab *ActiveBatch) {
	pb := PersistedBatch{
		ChatID:        chatID,
		StartPeer:     fmt.Sprintf("%v", ab.Data.StartPeer),
		StartID:       ab.Data.StartID,
		EndID:         ab.Data.EndID,
		LastSuccess:   ab.LastSuccess,
		TargetChat:    ab.Data.TargetChat,
		MediaFilter:   ab.Data.MediaFilter,
		Prefix:        ab.Data.Prefix,
		Suffix:        ab.Data.Suffix,
		ReplacePairs:  ab.Data.ReplacePairs,
		CaptionRemove: ab.Data.CaptionRemove,
		FileRemove:    ab.Data.FileRemove,
		IsClone:       ab.Data.IsClone,
	}

	filter := bson.M{"chat_id": chatID}
	update := bson.M{"$set": pb}
	opts := options.Update().SetUpsert(true)
	batchStateCol.UpdateOne(context.Background(), filter, update, opts)
}

func clearBatchState(chatID int64) {
	batchStateCol.DeleteOne(context.Background(), bson.M{"chat_id": chatID})
}

type CloneProgress struct {
	SourcePeer    string        `bson:"source_peer"`
	TargetChat    string        `bson:"target_chat"`
	LastMsgID     int           `bson:"last_msg_id"`
	LastLink      string        `bson:"last_link"`
	EndID         int           `bson:"end_id"`
	MediaFilter   string        `bson:"media_filter"`
	Prefix        string        `bson:"prefix"`
	Suffix        string        `bson:"suffix"`
	ReplacePairs  []ReplacePair `bson:"replace_pairs"`
	CaptionRemove string        `bson:"caption_remove"`
	FileRemove    string        `bson:"file_remove"`
	IsClone       bool          `bson:"is_clone"`
	UpdatedAt     time.Time     `bson:"updated_at"`
}

func saveCloneProgress(ab *ActiveBatch, msgID int, sourceLink string) {
	sourceKey := fmt.Sprintf("%v", ab.Data.StartPeer)
	cp := CloneProgress{
		SourcePeer:    sourceKey,
		TargetChat:    ab.Data.TargetChat,
		LastMsgID:     msgID,
		LastLink:      sourceLink,
		EndID:         ab.Data.EndID,
		MediaFilter:   ab.Data.MediaFilter,
		Prefix:        ab.Data.Prefix,
		Suffix:        ab.Data.Suffix,
		ReplacePairs:  ab.Data.ReplacePairs,
		CaptionRemove: ab.Data.CaptionRemove,
		FileRemove:    ab.Data.FileRemove,
		IsClone:       ab.Data.IsClone,
		UpdatedAt:     time.Now(),
	}

	filter := bson.M{"source_peer": sourceKey}
	update := bson.M{"$set": cp}
	opts := options.Update().SetUpsert(true)
	cloneProgressCol.UpdateOne(context.Background(), filter, update, opts)
}

type TopicNameMapping struct {
	TargetChat    string `bson:"target_chat"`
	SourceTopicID int32  `bson:"source_topic_id"`
	TopicName     string `bson:"topic_name"`
	DestTopic     int32  `bson:"dest_topic"`
}

func getDestTopicID(targetChat string, sourceTopicID int32, topicName string) int32 {
	var tm TopicNameMapping
	
	err := topicMapCol.FindOne(context.Background(), bson.M{
		"target_chat":     targetChat,
		"source_topic_id": sourceTopicID,
	}).Decode(&tm)
	if err == nil && tm.DestTopic != 0 {
		return tm.DestTopic
	}

	err = topicMapCol.FindOne(context.Background(), bson.M{
		"target_chat": targetChat,
		"topic_name":  topicName,
	}).Decode(&tm)
	if err == nil && tm.DestTopic != 0 {
		topicMapCol.UpdateOne(
			context.Background(),
			bson.M{"target_chat": targetChat, "topic_name": topicName},
			bson.M{"$set": bson.M{"source_topic_id": sourceTopicID}},
		)
		return tm.DestTopic
	}
	return 0
}

func saveTopicMapping(targetChat string, sourceTopicID int32, topicName string, destTopic int32) {
	tm := TopicNameMapping{
		TargetChat:    targetChat,
		SourceTopicID: sourceTopicID,
		TopicName:     topicName,
		DestTopic:     destTopic,
	}
	topicMapCol.InsertOne(context.Background(), tm)
}

func createTopicViaHTTP(targetChat string, title string) int32 {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/createForumTopic", BotToken)
	payload := map[string]interface{}{
		"chat_id": targetChat,
		"name":    title,
	}
	body, _ := json.Marshal(payload)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil || resp == nil {
		return 0
	}
	defer resp.Body.Close()

	resBody, _ := io.ReadAll(resp.Body)
	var res map[string]interface{}
	json.Unmarshal(resBody, &res)

	if ok, _ := res["ok"].(bool); ok {
		if result, ok2 := res["result"].(map[string]interface{}); ok2 {
			if tid, ok3 := result["message_thread_id"].(float64); ok3 {
				return int32(tid)
			}
		}
	}
	return 0
}

func deepSearchTitle(v reflect.Value) string {
	if !v.IsValid() {
		return ""
	}
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		return deepSearchTitle(v.Elem())
	}
	if v.Kind() == reflect.Struct {
		if f := v.FieldByName("Title"); f.IsValid() && f.Kind() == reflect.String {
			if f.String() != "" {
				return f.String()
			}
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if !field.CanInterface() {
				continue
			}
			name := v.Type().Field(i).Name
			if name == "Action" || name == "Message" {
				if res := deepSearchTitle(field); res != "" {
					return res
				}
			}
		}
	}
	return ""
}

func getTopicTitle(uc *telegram.Client, peer any, topicID int32) string {
	fallback := fmt.Sprintf("Topic_%d", topicID)
	if topicID == 0 || topicID == 1 {
		return "General"
	}
	msg, err := uc.GetMessageByID(peer, topicID)
	if err != nil || msg == nil {
		return fallback
	}
	title := deepSearchTitle(reflect.ValueOf(msg))
	if title != "" {
		return title
	}
	return fallback
}

func getOrCreateDestTopic(uc *telegram.Client, sourcePeer any, targetChatStr string, sourceTopicID int32) int32 {
	if sourceTopicID == 0 || sourceTopicID == 1 {
		return 0
	}
	topicTitle := getTopicTitle(uc, sourcePeer, sourceTopicID)
	
	destTopicID := getDestTopicID(targetChatStr, sourceTopicID, topicTitle)
	if destTopicID != 0 {
		return destTopicID
	}
	
	newTopicID := createTopicViaHTTP(targetChatStr, topicTitle)
	if newTopicID != 0 {
		saveTopicMapping(targetChatStr, sourceTopicID, topicTitle, newTopicID)
		return newTopicID
	}
	return 0
}

func colorProgressBar(percentage float64, action string) string {
	const width = 10
	if percentage < 0 {
		percentage = 0
	}
	if percentage > 100 {
		percentage = 100
	}

	filledCount := int((percentage / 100.0) * float64(width))
	emptyCount := width - filledCount

	var bar string
	if action == "download" {
		bar = strings.Repeat("🟦", filledCount)
	} else {
		for i := 0; i < filledCount; i++ {
			if i < 3 {
				bar += "🟥"
			} else if i < 7 {
				bar += "🟨"
			} else {
				bar += "🟩"
			}
		}
	}
	bar += strings.Repeat("⬜", emptyCount)
	return fmt.Sprintf("%s <code>%.1f%%</code>", bar, percentage)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func buildAdvancedProgress(label string, info *telegram.ProgressInfo, currentIdx int, sourceLink, snippet string, isClone bool, action string) string {
	batchStr := ""
	if isClone {
		batchStr = fmt.Sprintf("📊 <b>Clone Msg ID:</b> %d", currentIdx)
	} else {
		batchStr = fmt.Sprintf("📊 <b>Msg ID:</b> %d", currentIdx)
	}

	snippet = html.EscapeString(snippet)

	statsBlock := fmt.Sprintf("<pre><code class=\"language-bash\">📦 Size : %s / %s\n⚡ Speed: %s\n⏳ ETA  : %s</code></pre>",
		formatBytes(info.Current), formatBytes(info.TotalSize), info.SpeedString(), info.ETAString())

	infoBlock := fmt.Sprintf("<blockquote expandable>📄 <b>Info:</b> %s\n🔗 <b>Link:</b> <tg-spoiler><a href=\"%s\">Source</a></tg-spoiler>\n%s</blockquote>",
		snippet, sourceLink, batchStr)

	return fmt.Sprintf("<b>%s</b>\n\n%s\n%s\n%s",
		label, infoBlock, colorProgressBar(info.Percentage, action), statsBlock)
}

func editColoredProgressViaHTTP(chatID int64, msgID int32, text string) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", BotToken)
	payload := map[string]interface{}{
		"chat_id":    chatID,
		"message_id": msgID,
		"text":       text,
		"parse_mode": "HTML",
		"reply_markup": map[string]interface{}{
			"inline_keyboard": [][]map[string]interface{}{
				{
					{"text": "🔴 Stop Process", "callback_data": "stop_batch", "style": "danger"},
				},
			},
		},
	}
	body, _ := json.Marshal(payload)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err == nil && resp != nil {
		resp.Body.Close()
	}
}

type RateLimiter struct {
	LastUpdate time.Time
	Mu         sync.Mutex
}

func (rl *RateLimiter) ShouldUpdate() bool {
	rl.Mu.Lock()
	defer rl.Mu.Unlock()
	if time.Since(rl.LastUpdate) >= 7*time.Second {
		rl.LastUpdate = time.Now()
		return true
	}
	return false
}

func sendUnauthorizedMenu(chatID int64) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", BotToken)
	inlineKeyboard := [][]map[string]interface{}{
		{
			{"text": "📞 Contact User", "url": "https://t.me/H4R_Contact_bot"},
		},
		{
			{"text": "ℹ️ Advanced Info", "callback_data": "cmd_info", "style": "primary"},
		},
	}
	payload := map[string]interface{}{
		"chat_id": chatID,
		"text":    "Here is Noting if you want something then message on this bot @H4R_Contact_bot",
		"reply_markup": map[string]interface{}{"inline_keyboard": inlineKeyboard},
	}
	body, _ := json.Marshal(payload)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err == nil && resp != nil {
		resp.Body.Close()
	}
}

func sendAdvancedInfoViaHTTP(chatID int64) {
	chatUrl := fmt.Sprintf("https://api.telegram.org/bot%s/getChat", BotToken)
	payload := map[string]interface{}{"chat_id": chatID}
	body, _ := json.Marshal(payload)
	resp, err := httpClient.Post(chatUrl, "application/json", bytes.NewBuffer(body))
	
	var name, username string
	
	if err == nil && resp != nil {
		respBody, _ := io.ReadAll(resp.Body)
		var res map[string]interface{}
		json.Unmarshal(respBody, &res)
		if result, ok := res["result"].(map[string]interface{}); ok {
			if fName, ok := result["first_name"].(string); ok { name = fName }
			if lName, ok := result["last_name"].(string); ok { name += " " + lName }
			if uName, ok := result["username"].(string); ok { username = "@" + uName }
		}
		resp.Body.Close()
	}
	
	if username == "" { username = "None" }
	if name == "" { name = "Unknown User" }
	name = strings.TrimSpace(name)
	
	idStr := strconv.FormatInt(chatID, 10)
	idLen := len(idStr)
	
	creation := "Unknown"
	age := "Unknown"
	
	idFloat := float64(chatID)
	if idFloat < 100000000 {
		creation = "2013 - 2014"
		age = "10 - 11 years"
	} else if idFloat < 500000000 {
		creation = "2015 - 2017"
		age = "7 - 9 years"
	} else if idFloat < 1000000000 {
		creation = "2018 - 2019"
		age = "5 - 6 years"
	} else if idFloat < 2000000000 {
		creation = "2019 - 2020"
		age = "4 - 5 years"
	} else if idFloat < 5000000000 {
		creation = "2020 - 2021"
		age = "3 - 4 years"
	} else if idFloat < 6000000000 {
		creation = "2021 - 2022"
		age = "2 - 3 years"
	} else if idFloat < 7000000000 {
		creation = "2022 - 2023"
		age = "1 - 2 years"
	} else if idFloat < 8000000000 {
		creation = "2023 - 2024"
		age = "0 - 1 years"
	} else {
		creation = "2024+"
		age = "Less than a year"
	}

	botAccess := "Unauthorized ❌"
	if isOwner(chatID) {
		botAccess = "Admin (Owner) 👑"
	} else if isAdmin(chatID) {
		botAccess = "Authorized Admin ✅"
	}
	
	currentTime := time.Now().In(time.FixedZone("IST", 5*3600+1800)).Format("2006-01-02 15:04:05 MST")
	
	infoText := fmt.Sprintf(`🕵️‍♂️ <b>ADVANCED DNA PROFILING</b> 🕵️‍♂️
━━━━━━━━━━━━━━━━━━━━━━
🆔 <b>ID:</b> <code>%d</code> - %d Digits
👤 <b>Name:</b> %s
🔗 <b>Username:</b> %s
📅 <b>Created Est.:</b> %s
⏳ <b>Account Age:</b> %s
🌍 <b>Language:</b> English
⚠️ <b>Scam Label:</b> Safe ✅
🛑 <b>Fake Label:</b> Safe ✅
🕒 <b>Last Checked:</b> %s
🛡 <b>Bot Access:</b> %s
━━━━━━━━━━━━━━━━━━━━━━`, chatID, idLen, name, username, creation, age, currentTime, botAccess)

	sendUrl := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", BotToken)
	sendPayload := map[string]interface{}{
		"chat_id": chatID,
		"text": infoText,
		"parse_mode": "HTML",
	}
	sBody, _ := json.Marshal(sendPayload)
	sResp, _ := httpClient.Post(sendUrl, "application/json", bytes.NewBuffer(sBody))
	if sResp != nil { sResp.Body.Close() }
}

var (
	loginMu     sync.Mutex
	pendingByID = map[int64]*pendingLogin{}
	userClients = map[int64]*telegram.Client{}

	pendingPhoneMu     sync.Mutex
	pendingPhoneAction = map[int64]string{}

	pendingResumeMu   sync.Mutex
	pendingResumeData = map[int64]*CloneProgress{}

	pendingClearMu     sync.Mutex
	pendingClearTarget = map[int64]bool{}

	pendingAdminMu     sync.Mutex
	pendingAdminAction = map[int64]string{}

	batchMu       sync.Mutex
	batchByID     = map[int64]*BatchState{}
	activeBatchMu sync.Mutex
	activeBatches = map[int64]*ActiveBatch{}
	pausedBatches = map[int64]*ActiveBatch{}

	activeUserPhoneMu sync.Mutex
	activeUserPhone   = map[int64]string{}
	
	clientPoolMu sync.Mutex
	clientPool   = map[string]*telegram.Client{}
)

type loginStage int

const (
	stageAwaitingCode loginStage = iota
	stageAwaitingPassword
)

type pendingLogin struct {
	phone        string
	stage        loginStage
	codeChan     chan string
	passwordChan chan string
}

type BatchData struct {
	StartPeer     any
	StartID       int
	EndID         int
	TargetChat    string
	MediaFilter   string
	Prefix        string
	Suffix        string
	ReplacePairs  []ReplacePair
	CaptionRemove string
	FileRemove    string
	IsClone       bool
}
type BatchState struct {
	Awaiting string
	Data     *BatchData
}
type ActiveBatch struct {
	Data        *BatchData
	IsCancelled bool
	LastSuccess int
}

func getSourceLink(peer any, msgID int) string {
	if msgID <= 0 {
		return "None"
	}
	switch v := peer.(type) {
	case string:
		if strings.HasPrefix(v, "-100") {
			return fmt.Sprintf("https://t.me/c/%s/%d", strings.TrimPrefix(v, "-100"), msgID)
		}
		return fmt.Sprintf("https://t.me/%s/%d", strings.TrimPrefix(v, "@"), msgID)
	case int64:
		idStr := strconv.FormatInt(v, 10)
		if strings.HasPrefix(idStr, "-100") {
			return fmt.Sprintf("https://t.me/c/%s/%d", strings.TrimPrefix(idStr, "-100"), msgID)
		}
		return fmt.Sprintf("ID: %d", msgID)
	}
	return "Unknown"
}

func applyBranding(text, prefix, suffix string, isCaption bool) string {
	res := text
	if prefix != "" {
		if isCaption && res != "" {
			res = prefix + "\n\n" + res
		} else if isCaption {
			res = prefix
		} else {
			res = prefix + " " + res
		}
	}
	if suffix != "" {
		if isCaption && res != "" {
			res = res + "\n\n" + suffix
		} else if isCaption {
			res = suffix
		} else {
			res = res + " " + suffix
		}
	}
	return strings.TrimSpace(res)
}

func cleanCaption(text string, pairs []ReplacePair, remWord string) string {
	text = usernameRe.ReplaceAllString(text, "")
	text = urlRe.ReplaceAllString(text, "")
	
	if remWord != "" && strings.ToLower(remWord) != "skip" {
		wordsToRemove := strings.Split(remWord, ",")
		for _, w := range wordsToRemove {
			w = strings.TrimSpace(w)
			if w != "" {
				text = strings.ReplaceAll(text, w, "")
			}
		}
	}
	for _, p := range pairs {
		if p.Old == "" {
			continue
		}
		text = strings.ReplaceAll(text, p.Old, p.New)
	}
	return strings.TrimSpace(text)
}

func parseReplacePairs(text string) []ReplacePair {
	if strings.ToLower(strings.TrimSpace(text)) == "skip" {
		return nil
	}
	var pairs []ReplacePair
	entries := splitRe.Split(text, -1)
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "|", 2)
		if len(parts) != 2 {
			continue
		}
		old := strings.TrimSpace(parts[0])
		newVal := strings.TrimSpace(parts[1])
		if old == "" {
			continue
		}
		pairs = append(pairs, ReplacePair{Old: old, New: newVal})
	}
	return pairs
}

func normalizeTargetChat(raw string) string {
	if raw == "" {
		return raw
	}
	if strings.HasPrefix(raw, "@") || strings.HasPrefix(raw, "-100") {
		return raw
	}
	if _, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return "-100" + raw
	}
	return raw
}

func clearTargetCandidates(raw string) []string {
	raw = strings.TrimSpace(raw)
	normalized := normalizeTargetChat(raw)
	stripped := strings.TrimPrefix(normalized, "-100")

	seen := map[string]bool{}
	var candidates []string
	for _, c := range []string{raw, normalized, stripped} {
		if c != "" && !seen[c] {
			seen[c] = true
			candidates = append(candidates, c)
		}
	}
	return candidates
}

func doLogin(client *telegram.Client, chatID int64, phone string) {
	pl := &pendingLogin{phone: phone, stage: stageAwaitingCode, codeChan: make(chan string, 1), passwordChan: make(chan string, 1)}
	loginMu.Lock()
	pendingByID[chatID] = pl
	loginMu.Unlock()

	go func() {
		os.MkdirAll(sessionsDir, 0755)
		sessionFile := filepath.Join(sessionsDir, phone+".session")
		
		uc, err := telegram.NewClient(telegram.ClientConfig{
			AppID: ApiID, AppHash: ApiHash, SessionName: sessionFile,
		})
		if err != nil {
			client.SendMessage(chatID, "❌ Client init err: "+err.Error())
			loginMu.Lock()
			delete(pendingByID, chatID)
			loginMu.Unlock()
			return
		}
		uc.Conn()
		codeHash, err := uc.SendCode(phone)
		if err != nil {
			client.SendMessage(chatID, "❌ Code err: "+err.Error())
			loginMu.Lock()
			delete(pendingByID, chatID)
			loginMu.Unlock()
			return
		}
		client.SendMessage(chatID, "📲 OTP sent to "+phone+".\nType manually here.")

		opts := &telegram.LoginOptions{
			Ctx: context.Background(), CodeHash: codeHash,
			CodeCallback: func() (string, error) { 
				return <-pl.codeChan, nil 
			},
			PasswordCallback: func() (string, error) {
				loginMu.Lock()
				pl.stage = stageAwaitingPassword
				loginMu.Unlock()
				client.SendMessage(chatID, "🔐 Send your 2FA password.")
				return <-pl.passwordChan, nil
			},
		}

		_, err = telegram.CodeAuthAttempt(uc, phone, opts, 3)
		loginMu.Lock()
		delete(pendingByID, chatID)
		loginMu.Unlock()

		if err != nil {
			client.SendMessage(chatID, "❌ Login failed: "+err.Error())
			return
		}

		sessionStr := uc.ExportSession() 
		saveSessionToDB(phone, sessionStr, chatID)
		
		clientPoolMu.Lock()
		clientPool[phone] = uc
		clientPoolMu.Unlock()
		
		loginMu.Lock()
		userClients[chatID] = uc
		loginMu.Unlock()
		
		activeUserPhoneMu.Lock()
		activeUserPhone[chatID] = phone
		activeUserPhoneMu.Unlock()

		client.SendMessage(chatID, "✅ Logged in and saved to MongoDB. Use /switch "+phone+" next time.")
	}()
}

func doSwitch(client *telegram.Client, chatID int64, phone string) {
	sessionStr := getSessionFromDB(phone)
	if sessionStr == "" {
		client.SendMessage(chatID, "⚠️ Session not found in DB. Please /login first.")
		return
	}
	
	clientPoolMu.Lock()
	poolClient, exists := clientPool[phone]
	clientPoolMu.Unlock()

	if exists {
		loginMu.Lock()
		userClients[chatID] = poolClient
		loginMu.Unlock()
		
		activeUserPhoneMu.Lock()
		activeUserPhone[chatID] = phone
		activeUserPhoneMu.Unlock()
		
		client.SendMessage(chatID, "🔄 <b>Switched Successfully!</b> You are now using: "+phone, &telegram.SendOptions{ParseMode: "HTML"})
		return
	}

	os.MkdirAll(sessionsDir, 0755)
	sessionFile := filepath.Join(sessionsDir, phone+".session")
	
	var cfg telegram.ClientConfig
	if _, err := os.Stat(sessionFile); err == nil {
		cfg = telegram.ClientConfig{AppID: ApiID, AppHash: ApiHash, SessionName: sessionFile}
	} else {
		cfg = telegram.ClientConfig{AppID: ApiID, AppHash: ApiHash, StringSession: sessionStr, MemorySession: true}
	}

	var err error
	var uc *telegram.Client
	
	uc, err = telegram.NewClient(cfg)
	if err != nil {
		client.SendMessage(chatID, "❌ Failed to initialize client:\n<code>" + err.Error() + "</code>", &telegram.SendOptions{ParseMode: "HTML"})
		return
	}
	uc.Conn()
	
	clientPoolMu.Lock()
	clientPool[phone] = uc
	clientPoolMu.Unlock()
	
	loginMu.Lock()
	userClients[chatID] = uc
	loginMu.Unlock()
	
	activeUserPhoneMu.Lock()
	activeUserPhone[chatID] = phone
	activeUserPhoneMu.Unlock()

	client.SendMessage(chatID, "🔄 <b>Switched Successfully!</b> You are now using: "+phone, &telegram.SendOptions{ParseMode: "HTML"})
}

func doLogout(client *telegram.Client, chatID int64, phone string) {
	deleteSessionFromDB(phone)
	
	loginMu.Lock()
	delete(userClients, chatID)
	loginMu.Unlock()
	
	clientPoolMu.Lock()
	delete(clientPool, phone)
	clientPoolMu.Unlock()
	
	activeUserPhoneMu.Lock()
	if activeUserPhone[chatID] == phone {
		delete(activeUserPhone, chatID)
	}
	activeUserPhoneMu.Unlock()
	
	os.Remove(filepath.Join(sessionsDir, phone+".session"))
	
	client.SendMessage(chatID, "✅ Session for "+phone+" removed from MongoDB and Local Storage.")
}

func doResume(client *telegram.Client, chatID int64) {
	var pb PersistedBatch
	err := batchStateCol.FindOne(context.Background(), bson.M{"chat_id": chatID}).Decode(&pb)
	if err != nil {
		client.SendMessage(chatID, "⚠️ No crashed batch/clone found in DB to resume.")
		return
	}
	var startPeer any = pb.StartPeer
	if id, convErr := strconv.ParseInt(pb.StartPeer, 10, 64); convErr == nil {
		startPeer = id
	}
	ab := &ActiveBatch{
		Data: &BatchData{
			StartPeer: startPeer, StartID: pb.LastSuccess + 1, EndID: pb.EndID,
			TargetChat: pb.TargetChat, ReplacePairs: pb.ReplacePairs,
			CaptionRemove: pb.CaptionRemove, FileRemove: pb.FileRemove, IsClone: pb.IsClone,
			MediaFilter: pb.MediaFilter, Prefix: pb.Prefix, Suffix: pb.Suffix,
		},
		LastSuccess: pb.LastSuccess, IsCancelled: false,
	}
	activeBatchMu.Lock()
	activeBatches[chatID] = ab
	activeBatchMu.Unlock()

	client.SendMessage(chatID, fmt.Sprintf("🔄 <b>MongoDB State Found!</b>\nResuming process from ID <b>%d</b>...", ab.Data.StartID), &telegram.SendOptions{ParseMode: "HTML"})
	go runBatchProcess(client, chatID)
}

func doCancel(client *telegram.Client, chatID int64) {
	activeBatchMu.Lock()
	ab, inActiveBatch := activeBatches[chatID]
	if inActiveBatch {
		ab.IsCancelled = true
	}
	activeBatchMu.Unlock()
	if inActiveBatch {
		client.SendMessage(chatID, "🛑 Immediate Stop Signal Sent!")
	} else {
		client.SendMessage(chatID, "⚠️ No operation in progress to cancel.")
	}
}

func askForPhone(client *telegram.Client, chatID int64, action, prompt string) {
	pendingPhoneMu.Lock()
	pendingPhoneAction[chatID] = action
	pendingPhoneMu.Unlock()
	client.SendMessage(chatID, prompt, &telegram.SendOptions{ParseMode: "HTML"})
}

func askForClearTarget(client *telegram.Client, chatID int64) {
	pendingClearMu.Lock()
	pendingClearTarget[chatID] = true
	pendingClearMu.Unlock()
	client.SendMessage(chatID, "🧹 <b>Selective Cache Clear</b>\nSend the Destination Channel ID/Username to clear its topic + progress data.", &telegram.SendOptions{ParseMode: "HTML"})
}

func runBatchProcess(bot *telegram.Client, chatID int64) {
	loginMu.Lock()
	uc, ok := userClients[chatID]
	loginMu.Unlock()
	if !ok {
		return
	}

	activeBatchMu.Lock()
	ab, activeOk := activeBatches[chatID]
	activeBatchMu.Unlock()
	if !activeOk {
		return
	}

	taskStartTime := time.Now()

	modeStr := "Batch Mode"
	if ab.Data.IsClone {
		modeStr = "Full Clone Mode"
	}

	endLimit := ab.Data.EndID
	endStr := strconv.Itoa(endLimit)
	
	if ab.Data.IsClone && endLimit >= 5000000 {
		history, err := uc.GetHistory(ab.Data.StartPeer, &telegram.HistoryOption{Limit: 1})
		
		if err == nil && len(history) > 0 {
			endLimit = int(history[0].ID)
			endStr = fmt.Sprintf("%d (Exact Last ID 🎯)", endLimit)
		} else {
			endLimit = 999999999 
			endStr = "Latest (Auto-Detect 🔍)"
		}
	}

	bot.SendMessage(chatID, fmt.Sprintf("🚀 <b>%s Started</b>\nProcessing Msgs: %d to %s\n⚙️ Media Filter: <b>%s</b>", modeStr, ab.Data.StartID, endStr, strings.ToUpper(ab.Data.MediaFilter)), &telegram.SendOptions{ParseMode: "HTML"})

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var targetPeer any = chatID
	var targetChatStr string

	if ab.Data.TargetChat != "" {
		targetChatStr = normalizeTargetChat(ab.Data.TargetChat)
		if id, err := strconv.ParseInt(ab.Data.TargetChat, 10, 64); err == nil {
			targetPeer = id
		} else {
			targetPeer = ab.Data.TargetChat
		}
	} else {
		targetChatStr = strconv.FormatInt(chatID, 10)
	}

	processedCount := 0
	emptyCount := 0
	lastThreeHourBreak := time.Now()

	for msgID := ab.Data.StartID; msgID <= endLimit; msgID++ {
		activeBatchMu.Lock()
		if ab.IsCancelled {
			activeBatchMu.Unlock()
			break
		}
		activeBatchMu.Unlock()

		msg, err := uc.GetMessageByID(ab.Data.StartPeer, int32(msgID))
		
		if err != nil && strings.Contains(strings.ToUpper(err.Error()), "FLOOD_WAIT") {
			time.Sleep(15 * time.Second)
			msgID-- 
			continue
		}

		if err != nil || msg == nil || msg.Message == nil {
			emptyCount++
			
			if ab.Data.IsClone && emptyCount >= 15000 {
				bot.SendMessage(chatID, "🏁 <b>End of Group Reached (Auto-Detected)!</b> No more new messages found after 15,000 skipped IDs.", &telegram.SendOptions{ParseMode: "HTML"})
				break
			}

			sleepEmpty := 150 * time.Millisecond
			if emptyCount > 50 {
				sleepEmpty = 50 * time.Millisecond
			}
			if emptyCount > 500 {
				sleepEmpty = 10 * time.Millisecond
			}
			if emptyCount > 2000 {
				sleepEmpty = 2 * time.Millisecond 
			}
			time.Sleep(sleepEmpty)
			continue
		}
		
		emptyCount = 0 

		if !msg.IsMedia() {
			time.Sleep(50 * time.Millisecond) 
			continue
		}

		if ab.Data.MediaFilter != "all" && ab.Data.MediaFilter != "" && msg.Message.Media != nil {
			mediaVal := reflect.ValueOf(msg.Message.Media)
			if mediaVal.Kind() == reflect.Ptr {
				mediaVal = mediaVal.Elem()
			}
			mediaName := mediaVal.Type().Name()
			
			if ab.Data.MediaFilter == "video" && !strings.Contains(mediaName, "Document") && !strings.Contains(mediaName, "Video") {
				continue
			}
			if ab.Data.MediaFilter == "photo" && !strings.Contains(mediaName, "Photo") {
				continue
			}
			if ab.Data.MediaFilter == "audio" && !strings.Contains(mediaName, "Document") && !strings.Contains(mediaName, "Audio") {
				continue
			}
		}

		sourceLink := getSourceLink(ab.Data.StartPeer, msgID)

		var replyToTopicID int32 = 0
		if msg.Message.ReplyTo != nil {
			msgVal := reflect.ValueOf(msg.Message.ReplyTo)
			if msgVal.Kind() == reflect.Ptr {
				msgVal = msgVal.Elem()
			}
			if msgVal.Kind() == reflect.Struct {
				var sourceTopicID int32 = 0
				if topF := msgVal.FieldByName("ReplyToTopID"); topF.IsValid() && topF.CanInt() {
					sourceTopicID = int32(topF.Int())
				} else if topF = msgVal.FieldByName("ReplyToTopId"); topF.IsValid() && topF.CanInt() {
					sourceTopicID = int32(topF.Int())
				}
				
				if sourceTopicID == 0 {
					if msgF := msgVal.FieldByName("ReplyToMsgID"); msgF.IsValid() && msgF.CanInt() {
						sourceTopicID = int32(msgF.Int())
					} else if msgF = msgVal.FieldByName("ReplyToMsgId"); msgF.IsValid() && msgF.CanInt() {
						sourceTopicID = int32(msgF.Int())
					}
				}

				if sourceTopicID != 0 && sourceTopicID != 1 {
					replyToTopicID = getOrCreateDestTopic(uc, ab.Data.StartPeer, targetChatStr, sourceTopicID)
				}
			}
		}

		snippet := msg.Text()
		snippet = strings.ReplaceAll(snippet, "\n", " ")
		if len(snippet) > 35 {
			snippet = snippet[:35] + "..."
		}
		if snippet == "" {
			snippet = "Media File"
		}

		statusMsg, _ := bot.SendMessage(chatID, "Initializing...", &telegram.SendOptions{ParseMode: "HTML"})
		
		retryAttempts := []int{1, 2, 3}
		var filePath string
		var dlErr error
		var downloadedBytes int64

		for attempt, tryNum := range retryAttempts {
			statusText := fmt.Sprintf("<b>🔎 Processing ID %d...</b>\n<b>📥 Downloading (Try %d/3)...</b>\n%s", msgID, tryNum, colorProgressBar(0, "download"))
			editColoredProgressViaHTTP(chatID, statusMsg.ID, statusText)
			
			dlLimiter := &RateLimiter{LastUpdate: time.Now()}
			
			path, e := msg.Download(&telegram.DownloadOptions{
				ProgressInterval: 1,
				ProgressCallback: func(info *telegram.ProgressInfo) {
					activeBatchMu.Lock()
					isCanceled := ab.IsCancelled
					activeBatchMu.Unlock()
					if !isCanceled && (dlLimiter.ShouldUpdate() || info.Percentage >= 100) {
						go editColoredProgressViaHTTP(chatID, statusMsg.ID, buildAdvancedProgress(fmt.Sprintf("📥 Downloading ID %d... (Try %d/3)", msgID, tryNum), info, msgID, sourceLink, snippet, ab.Data.IsClone, "download"))
					}
					if info.Percentage >= 100 {
						downloadedBytes = info.Current
					}
				},
			})

			activeBatchMu.Lock()
			isCancelled := ab.IsCancelled
			activeBatchMu.Unlock()
			
			if isCancelled {
				if path != "" { 
					os.Remove(path) 
				}
				break
			}

			if e == nil {
				filePath = path
				dlErr = nil
				break
			}
			
			dlErr = e
			if attempt < len(retryAttempts)-1 {
				bot.SendMessage(chatID, fmt.Sprintf("⚠️ <b>Network Drop!</b> ID %d failed.\n🔄 Retrying (%d/3)...", msgID, tryNum+1), &telegram.SendOptions{ParseMode: "HTML"})
				time.Sleep(5 * time.Second)
			}
		}

		activeBatchMu.Lock()
		isCancelled := ab.IsCancelled
		activeBatchMu.Unlock()
		if isCancelled {
			if filePath != "" { 
				os.Remove(filePath) 
			}
			break
		}

		if dlErr != nil {
			bot.SendMessage(chatID, fmt.Sprintf("❌ <b>ID %d Download failed after all retries.</b>", msgID), &telegram.SendOptions{ParseMode: "HTML"})
			errMsg := fmt.Sprintf("⚠️ <b>Media File Skipped (Download Failed)</b>\n🔗 <b>Source:</b> %s\n🆔 <b>Msg ID:</b> %d", sourceLink, msgID)
			sendOpts := &telegram.SendOptions{ParseMode: "HTML"}
			if replyToTopicID != 0 {
				sendOpts.ReplyTo = &telegram.InputReplyToMessage{ReplyToMsgID: replyToTopicID}
			}
			bot.SendMessage(targetPeer, errMsg, sendOpts)
			ab.LastSuccess = msgID
			saveBatchState(chatID, ab)
			continue
		}

		dir := filepath.Dir(filePath)
		baseName := filepath.Base(filePath)
		ext := filepath.Ext(baseName)
		
		cleanName := cleanCaption(strings.TrimSuffix(baseName, ext), ab.Data.ReplacePairs, ab.Data.FileRemove)
		cleanName = applyBranding(cleanName, ab.Data.Prefix, ab.Data.Suffix, false)
		cleanName = illegalCharRe.ReplaceAllString(cleanName, "")
		cleanName = strings.TrimSpace(strings.ReplaceAll(cleanName, "  ", " "))
		if cleanName == "" {
			cleanName = fmt.Sprintf("media_%d", msgID)
		}

		newFilePath := filepath.Join(dir, cleanName+ext)
		if err := os.Rename(filePath, newFilePath); err == nil {
			filePath = newFilePath
		}

		thumbPath := filePath + ".jpg"
		cmdErr := exec.Command("ffmpeg", "-i", filePath, "-ss", "00:00:00.000", "-vframes", "1", thumbPath).Run()
		
		rawCaption := cleanCaption(msg.Text(), ab.Data.ReplacePairs, ab.Data.CaptionRemove)
		finalCaption := applyBranding(rawCaption, ab.Data.Prefix, ab.Data.Suffix, true)

		var upErr error
		
		for attempt, tryNum := range retryAttempts {
			editColoredProgressViaHTTP(chatID, statusMsg.ID, fmt.Sprintf("<b>📤 Uploading ID %d (Try %d/3)...</b>\n%s", msgID, tryNum, colorProgressBar(0, "upload")))
			
			upLimiter := &RateLimiter{LastUpdate: time.Now()}
			mediaOpts := &telegram.MediaOptions{Caption: finalCaption}
			if replyToTopicID != 0 {
				mediaOpts.ReplyTo = &telegram.InputReplyToMessage{ReplyToMsgID: replyToTopicID}
			}
			if cmdErr == nil {
				if _, err := os.Stat(thumbPath); err == nil {
					mediaOpts.Thumb = thumbPath
				}
			}

			mediaOpts.Upload = &telegram.UploadOptions{
				ProgressInterval: 1,
				ProgressCallback: func(info *telegram.ProgressInfo) {
					activeBatchMu.Lock()
					isCanceled := ab.IsCancelled
					activeBatchMu.Unlock()
					if !isCanceled && (upLimiter.ShouldUpdate() || info.Percentage >= 100) {
						go editColoredProgressViaHTTP(chatID, statusMsg.ID, buildAdvancedProgress(fmt.Sprintf("📤 Uploading ID %d... (Try %d/3)", msgID, tryNum), info, msgID, sourceLink, snippet, ab.Data.IsClone, "upload"))
					}
				},
			}

			_, e := bot.SendMedia(targetPeer, filePath, mediaOpts)

			activeBatchMu.Lock()
			isCancelled = ab.IsCancelled
			activeBatchMu.Unlock()
			
			if isCancelled { 
				break 
			}
			if e == nil { 
				upErr = nil
				break 
			}

			upErr = e
			if attempt < len(retryAttempts)-1 {
				bot.SendMessage(chatID, fmt.Sprintf("⚠️ <b>Network Drop on Upload!</b> ID %d failed.\n🔄 Retrying upload (%d/3)...", msgID, tryNum+1), &telegram.SendOptions{ParseMode: "HTML"})
				time.Sleep(5 * time.Second)
			}
		}

		if filePath != "" {
			os.Remove(filePath)
		}
		if cmdErr == nil { 
			os.Remove(thumbPath) 
		}

		activeBatchMu.Lock()
		isCancelled = ab.IsCancelled
		activeBatchMu.Unlock()
		if isCancelled { 
			break 
		}

		if upErr != nil {
			bot.SendMessage(chatID, fmt.Sprintf("❌ <b>ID %d Upload failed after all retries.</b>", msgID), &telegram.SendOptions{ParseMode: "HTML"})
			errMsg := fmt.Sprintf("⚠️ <b>Media File Skipped (Upload Failed)</b>\n🔗 <b>Source:</b> %s\n🆔 <b>Msg ID:</b> %d", sourceLink, msgID)
			sendOpts := &telegram.SendOptions{ParseMode: "HTML"}
			if replyToTopicID != 0 { 
				sendOpts.ReplyTo = &telegram.InputReplyToMessage{ReplyToMsgID: replyToTopicID} 
			}
			bot.SendMessage(targetPeer, errMsg, sendOpts)
			ab.LastSuccess = msgID
			saveBatchState(chatID, ab)
			continue
		}

		processedCount++
		incBotStats(downloadedBytes)

		activeBatchMu.Lock()
		ab.LastSuccess = msgID
		activeBatchMu.Unlock()
		saveBatchState(chatID, ab)
		saveCloneProgress(ab, msgID, sourceLink)

		sleepTime := 8.0 + rng.Float64()*7.0
		editColoredProgressViaHTTP(chatID, statusMsg.ID, fmt.Sprintf("✅ <b>ID %d Uploaded.</b>\n⏳ Taking %.1f sec break...", msgID, sleepTime))

		slept := 0.0
		for slept < sleepTime {
			time.Sleep(1 * time.Second)
			slept += 1.0
			activeBatchMu.Lock()
			if ab.IsCancelled { 
				activeBatchMu.Unlock()
				break 
			}
			activeBatchMu.Unlock()
		}

		activeBatchMu.Lock()
		if ab.IsCancelled {
			activeBatchMu.Unlock()
			bot.DeleteMessages(chatID, []int32{statusMsg.ID})
			break
		}
		activeBatchMu.Unlock()

		bot.DeleteMessages(chatID, []int32{statusMsg.ID})

		if time.Since(lastThreeHourBreak) >= 3*time.Hour {
			breakMsg, _ := bot.SendMessage(chatID, "🛡 <b>Safety Alert:</b> 3 hours continuous limit reached. Taking a 20-min break... 💤", &telegram.SendOptions{ParseMode: "HTML"})
			time.Sleep(20 * time.Minute)
			lastThreeHourBreak = time.Now()
			bot.DeleteMessages(chatID, []int32{breakMsg.ID})
		} else if processedCount > 0 && processedCount%30 == 0 {
			breakMsg, _ := bot.SendMessage(chatID, "🛡 <b>Cooldown:</b> 30 files processed. Taking a 5-min break... ⏳", &telegram.SendOptions{ParseMode: "HTML"})
			time.Sleep(5 * time.Minute)
			bot.DeleteMessages(chatID, []int32{breakMsg.ID})
		}
	}

	activeBatchMu.Lock()
	isCancelled := ab.IsCancelled
	lastSuccess := ab.LastSuccess
	activeBatchMu.Unlock()

	elapsedTime := time.Since(taskStartTime).Round(time.Second)

	if isCancelled {
		activeBatchMu.Lock()
		pausedBatches[chatID] = ab
		delete(activeBatches, chatID)
		activeBatchMu.Unlock()

		link := getSourceLink(ab.Data.StartPeer, lastSuccess)
		stopMsg := fmt.Sprintf("🛑 <b>Process Stopped!</b>\n\n✅ Last Success ID: <b>%d</b>\n🔗 Source: %s", lastSuccess, link)
		
		url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", BotToken)
		payload := map[string]interface{}{
			"chat_id":    chatID,
			"text":       stopMsg,
			"parse_mode": "HTML",
			"reply_markup": map[string]interface{}{
				"inline_keyboard": [][]map[string]interface{}{
					{{"text": "▶️ Resume", "callback_data": "resume_batch", "style": "success"}},
				},
			},
		}
		body, _ := json.Marshal(payload)
		resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
		if err == nil && resp != nil {
			resp.Body.Close()
		}
	} else {
		activeBatchMu.Lock()
		delete(activeBatches, chatID)
		delete(pausedBatches, chatID)
		activeBatchMu.Unlock()
		clearBatchState(chatID)

		receipt := fmt.Sprintf(`🧾 <b>VIP TASK SUMMARY REPORT</b>
━━━━━━━━━━━━━━━━━━━━━━
🎯 <b>Target:</b> %s
📦 <b>Total Cloned:</b> %d Files
⏱️ <b>Time Taken:</b> %s
━━━━━━━━━━━━━━━━━━━━━━
🎉 <i>All tasks completed successfully!</i>`, targetChatStr, processedCount, elapsedTime)
		
		bot.SendMessage(chatID, receipt, &telegram.SendOptions{ParseMode: "HTML"})
	}
}

func sendColoredMainMenuViaHTTP(chatID int64, userID int64, userName string) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", BotToken)

	var inlineKeyboard [][]map[string]interface{}

	inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
		{"text": "🔑 Login", "callback_data": "cmd_login", "style": "success"},
		{"text": "🔄 Switch", "callback_data": "cmd_switch", "style": "primary"},
	})
	inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
		{"text": "📱 Session List", "callback_data": "cmd_sessionlist", "style": "primary"},
		{"text": "🗑️ Logout", "callback_data": "cmd_logout", "style": "danger"},
	})
	inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
		{"text": "📦 Smart Batch", "callback_data": "init_batch", "style": "primary"},
		{"text": "🚀 Smart Clone", "callback_data": "init_clone", "style": "primary"},
	})
	inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
		{"text": "▶️ Resume", "callback_data": "cmd_resume", "style": "primary"},
		{"text": "📊 Stats", "callback_data": "cmd_stats", "style": "primary"},
	})
	inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
		{"text": "🛑 Cancel Task", "callback_data": "cmd_cancel", "style": "danger"},
		{"text": "🧹 Clear Cache", "callback_data": "cmd_cleartopics", "style": "primary"},
	})
	
	inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
		{"text": "ℹ️ Advanced Info", "callback_data": "cmd_info", "style": "primary"},
	})

	if isOwner(userID) {
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{
			{"text": "➕ Add Admin", "callback_data": "cmd_addadmin", "style": "success"},
			{"text": "➖ Remove Admin", "callback_data": "cmd_deladmin", "style": "danger"},
		})
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{{"text": "👥 Admin List", "callback_data": "cmd_listadmins", "style": "primary"}})
	} else if isAdmin(userID) {
		inlineKeyboard = append(inlineKeyboard, []map[string]interface{}{{"text": "👥 Admin List", "callback_data": "cmd_listadmins", "style": "primary"}})
	}

	text := fmt.Sprintf("🔥 Welcome to <b>VIP Cloner</b>, <b>%s</b>!\n\nSelect an option below to begin:", userName)

	payload := map[string]interface{}{
		"chat_id": chatID,
		"text":    text,
		"parse_mode": "HTML",
		"reply_markup": map[string]interface{}{"inline_keyboard": inlineKeyboard},
	}
	body, _ := json.Marshal(payload)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err == nil && resp != nil {
		resp.Body.Close()
	}
}

func sendMediaFilterMenu(chatID int64) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", BotToken)
	
	inlineKeyboard := [][]map[string]interface{}{
		{
			{"text": "📦 All Media", "callback_data": "filter_all"},
			{"text": "🎬 Videos Only", "callback_data": "filter_video"},
		},
		{
			{"text": "📄 Documents Only", "callback_data": "filter_doc"},
			{"text": "🖼 Photos Only", "callback_data": "filter_photo"},
		},
	}

	payload := map[string]interface{}{
		"chat_id": chatID,
		"text":    "✅ Target Saved.\n\n⚙️ <b>Next: Smart Media Filter</b>\nSelect what type of files you want to extract:",
		"parse_mode": "HTML",
		"reply_markup": map[string]interface{}{"inline_keyboard": inlineKeyboard},
	}
	body, _ := json.Marshal(payload)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err == nil && resp != nil {
		resp.Body.Close()
	}
}

func main() {
	// 🟢 ENV Loader
	loadEnvVariables()
	
	initMongoDB()

	// 🟢 Dummy Web Server to keep Render Web Service alive
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "VIP Cloner Bot is running perfectly!")
		})
		fmt.Println("🌐 Starting dummy HTTP server on port", port)
		http.ListenAndServe(":"+port, nil)
	}()

	client, err := telegram.NewClient(telegram.ClientConfig{
		AppID: ApiID, AppHash: ApiHash, SessionName: "bot", Session: "bot.session",
	})
	if err != nil {
		fmt.Println("❌ Client Error:", err)
		return
	}

	client.AddMessageHandler("/start", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		
		if !isAuthorized(chatID) { 
			sendUnauthorizedMenu(chatID)
			return nil 
		}
		
		userName := "Admin"
		if m.Sender != nil && m.Sender.FirstName != "" { 
			userName = m.Sender.FirstName 
		}
		sendColoredMainMenuViaHTTP(chatID, chatID, userName)
		return nil
	})

	client.AddMessageHandler("/login", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isAuthorized(chatID) {
			return nil
		}
		parts := strings.Fields(m.Text())
		if len(parts) < 2 {
			client.SendMessage(chatID, "Usage: /login +91xxxxxxxxxx")
			return nil
		}
		if !phoneRe.MatchString(parts[1]) {
			client.SendMessage(chatID, "❌ Invalid phone format. Example: +919876543210")
			return nil
		}
		doLogin(client, chatID, parts[1])
		return nil
	})

	client.AddMessageHandler("/switch", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isAuthorized(chatID) {
			return nil
		}
		parts := strings.Fields(m.Text())
		if len(parts) < 2 {
			client.SendMessage(chatID, "Usage: /switch +91xxxxxxxxxx")
			return nil
		}
		if !phoneRe.MatchString(parts[1]) {
			client.SendMessage(chatID, "❌ Invalid phone format.")
			return nil
		}
		doSwitch(client, chatID, parts[1])
		return nil
	})

	client.AddMessageHandler("/resume", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isAuthorized(chatID) {
			return nil
		}
		doResume(client, chatID)
		return nil
	})

	client.AddMessageHandler("/cancel", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isAuthorized(chatID) {
			return nil
		}
		doCancel(client, chatID)
		return nil
	})

	client.AddMessageHandler("/cleartopics", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isAuthorized(chatID) {
			return nil
		}
		askForClearTarget(client, chatID)
		return nil
	})

	client.AddMessageHandler("/addadmin", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isOwner(chatID) {
			return nil
		}
		parts := strings.Fields(m.Text())
		if len(parts) < 2 {
			client.SendMessage(chatID, "Usage: /addadmin <telegram_user_id>")
			return nil
		}
		newAdminID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			client.SendMessage(chatID, "❌ Invalid user ID. Must be numeric.")
			return nil
		}
		addAdmin(newAdminID, chatID)
		client.SendMessage(chatID, fmt.Sprintf("✅ <b>Admin Added!</b>\nUser ID <code>%d</code> now has full bot access.", newAdminID), &telegram.SendOptions{ParseMode: "HTML"})
		return nil
	})

	client.AddMessageHandler("/deladmin", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isOwner(chatID) {
			return nil
		}
		parts := strings.Fields(m.Text())
		if len(parts) < 2 {
			client.SendMessage(chatID, "Usage: /deladmin <telegram_user_id>")
			return nil
		}
		targetID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			client.SendMessage(chatID, "❌ Invalid user ID. Must be numeric.")
			return nil
		}
		removeAdmin(targetID)
		client.SendMessage(chatID, fmt.Sprintf("✅ <b>Admin Removed!</b>\nUser ID <code>%d</code> no longer has bot access.", targetID), &telegram.SendOptions{ParseMode: "HTML"})
		return nil
	})

	client.AddMessageHandler("/listadmins", func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		if !isOwner(chatID) {
			return nil
		}
		sendAdminList(client, chatID)
		return nil
	})

	client.AddMessageHandler(string(telegram.OnMessage), func(m *telegram.NewMessage) error {
		chatID := m.SenderID()
		
		if !isAuthorized(chatID) { 
			if chatID > 0 && !strings.HasPrefix(strings.TrimSpace(m.Text()), "/") {
				sendUnauthorizedMenu(chatID)
			}
			return nil 
		}
		
		text := strings.TrimSpace(m.Text())

		loginMu.Lock()
		pl, inLogin := pendingByID[chatID]
		loginMu.Unlock()
		if inLogin && text != "" && !strings.HasPrefix(text, "/") {
			if pl.stage == stageAwaitingCode { 
				pl.codeChan <- text 
			} else { 
				pl.passwordChan <- text 
			}
			return nil
		}

		pendingAdminMu.Lock()
		adminAction, awaitingAdmin := pendingAdminAction[chatID]
		pendingAdminMu.Unlock()
		if awaitingAdmin && text != "" && !strings.HasPrefix(text, "/") {
			pendingAdminMu.Lock()
			delete(pendingAdminAction, chatID)
			pendingAdminMu.Unlock()

			targetID, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				client.SendMessage(chatID, "❌ Invalid user ID. Must be numeric.")
				return nil
			}

			if adminAction == "add" {
				addAdmin(targetID, chatID)
				client.SendMessage(chatID, fmt.Sprintf("✅ <b>Admin Added!</b>\nUser ID <code>%d</code> now has full bot access.", targetID), &telegram.SendOptions{ParseMode: "HTML"})
			} else if adminAction == "del" {
				removeAdmin(targetID)
				client.SendMessage(chatID, fmt.Sprintf("✅ <b>Admin Removed!</b>\nUser ID <code>%d</code> no longer has bot access.", targetID), &telegram.SendOptions{ParseMode: "HTML"})
			}
			return nil
		}

		pendingPhoneMu.Lock()
		action, awaitingPhone := pendingPhoneAction[chatID]
		pendingPhoneMu.Unlock()
		if awaitingPhone && text != "" && !strings.HasPrefix(text, "/") {
			if !phoneRe.MatchString(text) {
				client.SendMessage(chatID, "❌ Invalid phone format.")
				return nil
			}
			pendingPhoneMu.Lock()
			delete(pendingPhoneAction, chatID)
			pendingPhoneMu.Unlock()
			switch action {
			case "login":
				doLogin(client, chatID, text)
			case "switch":
				doSwitch(client, chatID, text)
			case "logout":
				doLogout(client, chatID, text)
			}
			return nil
		}

		pendingClearMu.Lock()
		awaitingClear := pendingClearTarget[chatID]
		pendingClearMu.Unlock()
		if awaitingClear && text != "" && !strings.HasPrefix(text, "/") {
			pendingClearMu.Lock()
			delete(pendingClearTarget, chatID)
			pendingClearMu.Unlock()

			candidates := clearTargetCandidates(text)
			topicRes, _ := topicMapCol.DeleteMany(context.Background(), bson.M{"target_chat": bson.M{"$in": candidates}})
			batchRes, _ := batchStateCol.DeleteMany(context.Background(), bson.M{"target_chat": bson.M{"$in": candidates}})
			cloneRes, _ := cloneProgressCol.DeleteMany(context.Background(), bson.M{"target_chat": bson.M{"$in": candidates}})

			var topicCount, batchCount, cloneCount int64
			if topicRes != nil {
				topicCount = topicRes.DeletedCount
			}
			if batchRes != nil {
				batchCount = batchRes.DeletedCount
			}
			if cloneRes != nil {
				cloneCount = cloneRes.DeletedCount
			}

			client.SendMessage(chatID, fmt.Sprintf("✅ <b>Cache Cleared for %s!</b>\n🗺️ Topic Mappings Removed: %d\n💾 Batch States Removed: %d\n📍 Clone Progress Removed: %d", text, topicCount, batchCount, cloneCount), &telegram.SendOptions{ParseMode: "HTML"})
			return nil
		}

		batchMu.Lock()
		bState, inBatch := batchByID[chatID]
		batchMu.Unlock()

		if inBatch && bState.Awaiting != "" {
			switch bState.Awaiting {

			case "batch_start_link":
				match := tmeLinkRe.FindStringSubmatch(text)
				if match == nil { 
					return nil 
				}
				bState.Data.StartPeer = match[2]
				bState.Data.StartID, _ = strconv.Atoi(match[3])
				bState.Awaiting = "batch_end_link"
				client.SendMessage(chatID, "✅ Start ID saved. \n🔗 <b>Step 2:</b> Ending message link.", &telegram.SendOptions{ParseMode: "HTML"})
			
			case "batch_end_link":
				match := tmeLinkRe.FindStringSubmatch(text)
				if match == nil { 
					return nil 
				}
				bState.Data.EndID, _ = strconv.Atoi(match[3])
				bState.Awaiting = "shared_target"
				client.SendMessage(chatID, "✅ End ID saved.\n📢 <b>Step 3:</b> Target Channel ID/Username.", &telegram.SendOptions{ParseMode: "HTML"})

			case "clone_source":
				sourceKey := text
				var cp CloneProgress
				lookupErr := cloneProgressCol.FindOne(context.Background(), bson.M{"source_peer": sourceKey}).Decode(&cp)
				if lookupErr == nil {
					pendingResumeMu.Lock()
					pendingResumeData[chatID] = &cp
					pendingResumeMu.Unlock()
					bState.Data.StartPeer = sourceKey
					bState.Awaiting = "clone_resume_choice"
					client.SendMessage(chatID, fmt.Sprintf("⚠️ <b>Old Data Found!</b>\n\n📍 Last Msg ID: <b>%d</b>\n🎯 Prev Target: %s\n\nReply <b>0</b> to restart, <b>1</b> to resume, or paste a t.me link.", cp.LastMsgID, cp.TargetChat), &telegram.SendOptions{ParseMode: "HTML"})
				} else {
					bState.Data.StartPeer = sourceKey
					bState.Data.StartID = 1
					bState.Data.EndID = 999999999 
					bState.Data.IsClone = true
					bState.Awaiting = "shared_target"
					client.SendMessage(chatID, "✅ Source Saved.\n📢 <b>Next:</b> Target Channel ID/Username.", &telegram.SendOptions{ParseMode: "HTML"})
				}

			case "clone_resume_choice":
				pendingResumeMu.Lock()
				cp, found := pendingResumeData[chatID]
				delete(pendingResumeData, chatID)
				pendingResumeMu.Unlock()

				if !found {
					bState.Data.StartID = 1
					bState.Data.EndID = 999999999
					bState.Data.IsClone = true
					bState.Awaiting = "shared_target"
					client.SendMessage(chatID, "⚠️ Session expired. Send Target Channel ID.")
					return nil
				}

				startID := 1
				choice := strings.TrimSpace(text)
				if choice == "1" {
					startID = cp.LastMsgID + 1
				} else if choice != "0" {
					match := tmeLinkRe.FindStringSubmatch(text)
					if match != nil { 
						startID, _ = strconv.Atoi(match[3]) 
					}
				}

				endID := cp.EndID
				if endID == 0 { 
					endID = 999999999 
				}

				bState.Data.StartID = startID
				bState.Data.EndID = endID
				bState.Data.TargetChat = cp.TargetChat
				bState.Data.MediaFilter = cp.MediaFilter
				bState.Data.Prefix = cp.Prefix
				bState.Data.Suffix = cp.Suffix
				bState.Data.ReplacePairs = cp.ReplacePairs
				bState.Data.CaptionRemove = cp.CaptionRemove
				bState.Data.FileRemove = cp.FileRemove
				bState.Data.IsClone = true

				bState.Awaiting = "shared_replace"
				client.SendMessage(chatID, fmt.Sprintf("✅ Resuming from ID <b>%d</b>.\n🔄 <b>Next:</b> Replace Words (<code>Old|New</code>) or <code>skip</code>.", startID), &telegram.SendOptions{ParseMode: "HTML"})

			case "shared_target":
				bState.Data.TargetChat = text
				bState.Awaiting = "awaiting_filter_callback"
				sendMediaFilterMenu(chatID) 
			
			case "shared_prefix":
				if strings.ToLower(text) != "skip" { 
					bState.Data.Prefix = text 
				}
				bState.Awaiting = "shared_suffix"
				client.SendMessage(chatID, "✅ Prefix Saved.\n🔤 <b>Next: Custom Suffix</b>\nSend text to add at the END of caption/filename, or send <code>skip</code>.", &telegram.SendOptions{ParseMode: "HTML"})
			
			case "shared_suffix":
				if strings.ToLower(text) != "skip" { 
					bState.Data.Suffix = text 
				}
				bState.Awaiting = "shared_replace"
				client.SendMessage(chatID, "✅ Suffix Saved.\n🔄 <b>Next: Replace Words</b>\nSend pairs like <code>Old | New</code> or send <code>skip</code>.", &telegram.SendOptions{ParseMode: "HTML"})
			
			case "shared_replace":
				if strings.ToLower(text) != "skip" { 
					bState.Data.ReplacePairs = parseReplacePairs(text) 
				}
				bState.Awaiting = "shared_caption_remove"
				client.SendMessage(chatID, "✅ Saved.\n🗑 <b>Next: Caption Word Remove</b>\nSend comma-separated words (<code>w1, w2</code>) or <code>skip</code>.", &telegram.SendOptions{ParseMode: "HTML"})
			
			case "shared_caption_remove":
				if strings.ToLower(text) != "skip" { 
					bState.Data.CaptionRemove = text 
				}
				bState.Awaiting = "shared_file_remove"
				client.SendMessage(chatID, "✅ Saved.\n📄 <b>Next: Filename Word Remove</b>\nSend comma-separated words or <code>skip</code>.", &telegram.SendOptions{ParseMode: "HTML"})
			
			case "shared_file_remove":
				if strings.ToLower(text) != "skip" { 
					bState.Data.FileRemove = text 
				}

				activeBatchMu.Lock()
				activeBatches[chatID] = &ActiveBatch{Data: bState.Data, LastSuccess: bState.Data.StartID - 1}
				activeBatchMu.Unlock()
				batchMu.Lock()
				delete(batchByID, chatID)
				batchMu.Unlock()
				go runBatchProcess(client, chatID)
			}
		}
		return nil
	})

	client.On("callback:", func(cb *telegram.CallbackQuery) error {
		cmd := string(cb.Data)

		if !isAuthorized(cb.SenderID) && cmd != "cmd_info" { 
			cb.Answer("⛔ Unauthorized Action!")
			return nil 
		}

		if strings.HasPrefix(cmd, "filter_") {
			filterType := strings.TrimPrefix(cmd, "filter_")
			batchMu.Lock()
			bState, inBatch := batchByID[cb.SenderID]
			if inBatch && bState.Awaiting == "awaiting_filter_callback" {
				bState.Data.MediaFilter = filterType
				bState.Awaiting = "shared_prefix"
				
				client.SendMessage(cb.SenderID, fmt.Sprintf("✅ Filter '<b>%s</b>' Saved.\n\n🔡 <b>Next: Custom Prefix</b>\nSend the text you want to add at the BEGINNING of caption/filename. Send <code>skip</code> to ignore.", strings.ToUpper(filterType)), &telegram.SendOptions{ParseMode: "HTML"})
			}
			batchMu.Unlock()
			cb.Answer("✅ Filter selected")
			return nil
		}

		switch cmd {
		case "cmd_info":
			cb.Answer("🔍 Fetching DNA Profile...")
			go sendAdvancedInfoViaHTTP(cb.SenderID)

		case "cmd_stats":
			var s BotStats
			err := botStatsCol.FindOne(context.Background(), bson.M{"_id": "global_stats"}).Decode(&s)
			if err == nil {
				uptime := time.Since(s.StartTime).Round(time.Minute)
				statsText := fmt.Sprintf("📊 <b>Live Analytics Dashboard</b>\n\n📦 <b>Total Files Processed:</b> %d\n💾 <b>Total Data Handled:</b> %s\n⏱ <b>Server Uptime:</b> %s", s.TotalFiles, formatBytes(s.TotalBytes), uptime.String())
				client.SendMessage(cb.SenderID, statsText, &telegram.SendOptions{ParseMode: "HTML"})
				cb.Answer("📊 Stats Loaded")
			} else {
				cb.Answer("⚠️ Stats not available yet")
			}

		case "init_batch":
			batchMu.Lock()
			batchByID[cb.SenderID] = &BatchState{Awaiting: "batch_start_link", Data: &BatchData{}}
			batchMu.Unlock()
			client.SendMessage(cb.SenderID, "📦 <b>Smart Batch Mode</b>\n\n🔗 <b>Step 1:</b> Send the starting message link.", &telegram.SendOptions{ParseMode: "HTML"})
		
		case "init_clone":
			batchMu.Lock()
			batchByID[cb.SenderID] = &BatchState{Awaiting: "clone_source", Data: &BatchData{}}
			batchMu.Unlock()
			client.SendMessage(cb.SenderID, "🚀 <b>Smart Group Clone</b>\n\n🔗 <b>Step 1:</b> Send the Source Channel ID or Username.", &telegram.SendOptions{ParseMode: "HTML"})
		
		case "cmd_login": 
			cb.Answer("📱 Send phone number")
			askForPhone(client, cb.SenderID, "login", "🔑 <b>Login</b>\nSend phone (+9198...)")
		
		case "cmd_switch": 
			cb.Answer("📱 Send phone number")
			askForPhone(client, cb.SenderID, "switch", "🔄 <b>Switch Account</b>\nSend phone (+9198...)")
		
		case "cmd_logout": 
			cb.Answer("📱 Send phone number")
			askForPhone(client, cb.SenderID, "logout", "🗑️ <b>Logout</b>\nSend phone (+9198...)")
		
		case "cmd_resume": 
			cb.Answer("▶️ Resuming...")
			doResume(client, cb.SenderID)
		
		case "cmd_cancel": 
			cb.Answer("🛑 Cancelled")
			doCancel(client, cb.SenderID)
		
		case "cmd_cleartopics": 
			cb.Answer("🧹 Send destination")
			askForClearTarget(client, cb.SenderID)
		
		case "cmd_addadmin":
			if !isOwner(cb.SenderID) {
				cb.Answer("⛔ Owner only")
				return nil
			}
			pendingAdminMu.Lock()
			pendingAdminAction[cb.SenderID] = "add"
			pendingAdminMu.Unlock()
			client.SendMessage(cb.SenderID, "➕ <b>Add Admin</b>\nSend the Telegram User ID you want to make an admin.", &telegram.SendOptions{ParseMode: "HTML"})
			cb.Answer("Send User ID")
			
		case "cmd_deladmin":
			if !isOwner(cb.SenderID) {
				cb.Answer("⛔ Owner only")
				return nil
			}
			pendingAdminMu.Lock()
			pendingAdminAction[cb.SenderID] = "del"
			pendingAdminMu.Unlock()
			client.SendMessage(cb.SenderID, "➖ <b>Remove Admin</b>\nSend the Telegram User ID you want to remove.", &telegram.SendOptions{ParseMode: "HTML"})
			cb.Answer("Send User ID")
			
		case "cmd_listadmins":
			if !isOwner(cb.SenderID) {
				cb.Answer("⛔ Owner only")
				return nil
			}
			sendAdminList(client, cb.SenderID)
			cb.Answer("👥 Admin list sent")

		case "stop_batch":
			activeBatchMu.Lock()
			ab, ok := activeBatches[cb.SenderID]
			if ok { 
				ab.IsCancelled = true 
			}
			activeBatchMu.Unlock()
			cb.Answer("🔴 Stopping...")
			
		case "resume_batch":
			activeBatchMu.Lock()
			ab, ok := pausedBatches[cb.SenderID]
			activeBatchMu.Unlock()
			if ok {
				ab.Data.StartID = ab.LastSuccess + 1
				ab.IsCancelled = false
				activeBatchMu.Lock()
				activeBatches[cb.SenderID] = ab
				delete(pausedBatches, cb.SenderID)
				activeBatchMu.Unlock()
				go runBatchProcess(client, cb.SenderID)
				cb.Answer("▶️ Resuming...")
			}

		case "cmd_sessionlist":
			cur, err := sessionsCol.Find(context.Background(), bson.M{"owner_chat_id": cb.SenderID})
			if err != nil {
				client.SendMessage(cb.SenderID, "❌ Failed to fetch sessions.")
				return nil
			}
			defer cur.Close(context.Background())
			
			var sessions []UserSession
			if err = cur.All(context.Background(), &sessions); err != nil {
				client.SendMessage(cb.SenderID, "❌ Failed to read sessions.")
				return nil
			}
			
			if len(sessions) == 0 {
				client.SendMessage(cb.SenderID, "⚠️ No accounts logged in yet.", &telegram.SendOptions{ParseMode: "HTML"})
				cb.Answer("No sessions found")
				return nil
			}

			activeUserPhoneMu.Lock()
			activePhone := activeUserPhone[cb.SenderID]
			activeUserPhoneMu.Unlock()

			var msgLines []string
			msgLines = append(msgLines, "📱 <b>Your Logged-in Sessions:</b>\n")
			
			for _, s := range sessions {
				if s.Phone == activePhone {
					msgLines = append(msgLines, fmt.Sprintf("🟢 <b>%s</b> <i>(Active/Running)</i>", s.Phone))
				} else {
					msgLines = append(msgLines, fmt.Sprintf("⚪ <code>%s</code>", s.Phone))
				}
			}
			
			msgLines = append(msgLines, "\n<i>Use /switch &lt;number&gt; to change active session.</i>")
			client.SendMessage(cb.SenderID, strings.Join(msgLines, "\n"), &telegram.SendOptions{ParseMode: "HTML"})
			cb.Answer("Session list sent")
		}
		return nil
	})

	client.LoginBot(BotToken)
	fmt.Println("🚀 VIP Cloner Bot is listening...")
	client.Idle()
}
