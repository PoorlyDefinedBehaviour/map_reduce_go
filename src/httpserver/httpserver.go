package httpserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
)

// HTTP server used to receive new task requests from clients.
type HTTPServer struct {
	mux    *http.ServeMux
	master *master.Master
}

func New(master *master.Master) *HTTPServer {
	srv := &HTTPServer{
		mux:    http.NewServeMux(),
		master: master,
	}

	srv.mux.HandleFunc("POST /task", srv.handleNewTask)

	return srv
}

func (srv *HTTPServer) Start(addr string) error {
	fmt.Printf("starting http server: addr=%s\n", addr)
	if err := http.ListenAndServe(addr, srv.mux); err != nil {
		return fmt.Errorf("listening on addr: addr=%s %w", addr, err)
	}
	return nil
}

type NewTaskRequest struct {
	File                string `json:"file"`
	NumberOfPartitions  uint16 `json:"numberOfPartitions"`
	NumberOfMapTasks    uint16 `json:"numberOfMapTasks"`
	NumberOfReduceTasks uint16 `json:"numberOfReduceTasks"`
	ScriptBase64        string `json:"scriptBase64"`
}

func (srv *HTTPServer) handleNewTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var newTaskRequest NewTaskRequest

	if err := json.NewDecoder(r.Body).Decode(&newTaskRequest); err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	scriptString, err := base64.StdEncoding.DecodeString(newTaskRequest.ScriptBase64)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	// script, err := javascript.Parse(string(scriptString))
	// if err != nil {
	// 	_, _ = w.Write([]byte(err.Error()))
	// 	w.WriteHeader(http.StatusInternalServerError)

	// 	return
	// }
	// defer script.Close()

	validatedInput, err := master.NewValidatedInput(contracts.Input{
		File:                newTaskRequest.File,
		Script:              string(scriptString),
		NumberOfMapTasks:    uint32(newTaskRequest.NumberOfMapTasks),
		NumberOfReduceTasks: uint32(newTaskRequest.NumberOfReduceTasks),
		NumberOfPartitions:  uint32(newTaskRequest.NumberOfPartitions),
	})
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	if err := srv.master.Run(ctx, validatedInput); err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}
}
