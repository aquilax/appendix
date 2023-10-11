package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type NodeID string
type Namespace string // dataset.v1
type MessageID string
type MessageOperator string
type Timestamp int64

const MessageOperatorAdd MessageOperator = "ADD"
const MessageOperatorUpdate MessageOperator = "UPD"
const MessageOperatorDelete MessageOperator = "DEL"

type MessageMeta struct {
	NodeID    NodeID          `json:"node_id"`
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

func main() {
	app := NewApp()
	mux := http.NewServeMux()
	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		var p Payload
		err := json.NewDecoder(r.Body).Decode(&p)
		if err != nil {
			fmt.Println(err)
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
	storage []Message
	ids     map[MessageID]any
}

func NewApp() *App {
	return &App{
		storage: make([]Message, 0),
		ids:     make(map[MessageID]any),
	}
}

func (a *App) processPayload(p *Payload) (*Payload, error) {
	err := a.saveRequest(p)
	if err != nil {
		return nil, err
	}
	return a.getResponse(p.Cursor)
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
		log.Println(msg)
	}
	return nil
}

func (a *App) getResponse(messageID MessageID) (*Payload, error) {
	a.RLock()
	defer a.RUnlock()

	for i := len(a.storage) - 1; i >= 0; i-- {
		if a.storage[i].ID == messageID {
			return &Payload{
				Messages: a.storage[i+1:],
			}, nil
		}
	}
	return &Payload{
		Messages: a.storage[:],
	}, nil
}
