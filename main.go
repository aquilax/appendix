package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const ENV_APPENDIX_LOG = "APPENDIX_LOG"
const ENV_API_TOKEN = "API_TOKEN"
const EmptyMessageID MessageID = "-"

var newLine = []byte{10}

type NodeID string
type Namespace string // dataset.v1
type MessageID string
type MessageOperator string
type Timestamp int64

const MessageOperatorAdd MessageOperator = "ADD"
const MessageOperatorUpdate MessageOperator = "UPD"
const MessageOperatorDelete MessageOperator = "DEL"

// messageId = "nsid.nodeid.messageID"

type MessageMeta struct {
	Namespace Namespace       `json:"ns"`
	Operation MessageOperator `json:"op"`
	MessageID MessageID       `json:"message_id"`
	Timestamp Timestamp       `json:"ts"`
}

type Message struct {
	ID   MessageID       `json:"id"`
	Meta MessageMeta     `json:"meta"`
	Data json.RawMessage `json:"data"`
}

type Payload struct {
	Cursor   MessageID `json:"cursor"`
	Messages []Message `json:"messages"`
}

func (p *Payload) UnmarshalJSON(data []byte) error {
	required := struct {
		Cursor   *MessageID `json:"cursor"`
		Messages []Message  `json:"messages"`
	}{}
	if err := json.Unmarshal(data, &required); err != nil {
		return err
	} else if required.Cursor == nil {
		return errors.New("missing: cursor")
	} else if required.Messages == nil {
		return errors.New("missing: messages")
	}
	p.Cursor = *required.Cursor
	p.Messages = required.Messages
	return nil
}

func main() {
	app := NewApp(os.Stdout)
	apiToken := os.Getenv(ENV_API_TOKEN)
	err := app.loadStorage(os.Getenv(ENV_APPENDIX_LOG))
	if err != nil {
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-NodeID")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == http.MethodOptions {
			return // Preflight
		}

		if r.Method != http.MethodPost {
			http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		authorization := r.Header.Get("Authorization")
		idToken := strings.TrimSpace(strings.Replace(authorization, "Bearer", "", 1))
		if apiToken != "" && apiToken != idToken {
			http.Error(w, `{"error": "forbidden"}`, http.StatusForbidden)
			return
		}

		var p Payload
		err := json.NewDecoder(r.Body).Decode(&p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		response, err := app.processPayload(&p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	srv := &http.Server{
		Addr:    ":3333",
		Handler: mux,
	}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			os.Exit(1)
		}
	}()

	<-done

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
	}()

	if err := srv.Shutdown(ctx); err != nil {
		os.Exit(2)
	}
}

var ErrBadRequest = errors.New(`{"error": "bad request"}`)

type App struct {
	sync.RWMutex
	output  io.Writer
	storage []Message
	ids     map[MessageID]any
}

func NewApp(log io.Writer) *App {
	return &App{
		output:  log,
		storage: make([]Message, 0),
		ids:     make(map[MessageID]any),
	}
}

func (a *App) processPayload(p *Payload) (*Payload, error) {
	messages := a.getMessagesAfter(p.Cursor)

	err := a.saveRequest(p)
	if err != nil {
		return nil, err
	}
	return a.getResponse(p.Cursor, messages), nil
}

func (a *App) saveRequest(p *Payload) error {
	a.Lock()
	defer a.Unlock()

	for i := range p.Messages {
		if _, found := a.ids[p.Messages[i].ID]; found {
			continue
		}
		a.storage = append(a.storage, p.Messages[i])
		a.ids[p.Messages[i].ID] = nil
		msg, err := json.Marshal(p.Messages[i])
		if err != nil {
			return ErrBadRequest
		}
		if _, err := a.output.Write(msg); err != nil {
			panic(err)
		}
		if _, err = a.output.Write(newLine); err != nil {
			panic(err)
		}
	}
	return nil
}

func (a *App) getResponse(messageID MessageID, messages []Message) *Payload {
	return &Payload{
		Cursor: func() MessageID {
			l := len(messages)
			if l > 0 {
				return messages[l-1].ID
			}
			return EmptyMessageID
		}(),
		Messages: messages,
	}
}

func (a *App) getMessagesAfter(messageID MessageID) []Message {
	a.RLock()
	defer a.RUnlock()

	for i := len(a.storage) - 1; i >= 0; i-- {
		if a.storage[i].ID == messageID {
			return a.storage[i+1:]
		}
	}
	return a.storage[:]
}

func (a *App) loadStorage(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	d := json.NewDecoder(bufio.NewReader(f))
	for {
		var m Message
		if err := d.Decode(&m); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if _, found := a.ids[m.ID]; found {
			continue
		}
		a.ids[m.ID] = nil
		a.storage = append(a.storage, m)
	}
}
