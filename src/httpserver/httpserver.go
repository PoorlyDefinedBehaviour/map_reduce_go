package httpserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"net/http"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	io "github.com/poorlydefinedbehaviour/map_reduce_go/src/io_handler"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/memory"
)

// HTTP server used to receive new task requests from clients.
type HTTPServer struct {
	mux    *http.ServeMux
	master *io.MasterIOHandler
}

func New(master *io.MasterIOHandler) *HTTPServer {
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
	File                string   `json:"file"`
	NumberOfPartitions  uint16   `json:"numberOfPartitions"`
	NumberOfMapTasks    uint16   `json:"numberOfMapTasks"`
	NumberOfReduceTasks uint16   `json:"numberOfReduceTasks"`
	Requests            Requests `json:"requests"`
	ScriptBase64        string   `json:"scriptBase64"`
}

type Requests struct {
	Memory string `json:"memory"`
}

func (srv *HTTPServer) handleNewTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var newTaskRequest NewTaskRequest

	if err := json.NewDecoder(r.Body).Decode(&newTaskRequest); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	scriptString, err := base64.StdEncoding.DecodeString(newTaskRequest.ScriptBase64)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	requestsMemory, err := memory.FromStringToBytes(newTaskRequest.Requests.Memory)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	validatedInput, err := master.NewValidatedInput(contracts.Input{
		File:                newTaskRequest.File,
		Script:              string(scriptString),
		NumberOfMapTasks:    uint32(newTaskRequest.NumberOfMapTasks),
		NumberOfReduceTasks: uint32(newTaskRequest.NumberOfReduceTasks),
		NumberOfPartitions:  uint32(newTaskRequest.NumberOfPartitions),
		RequestsMemory:      requestsMemory,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	result, err := srv.master.ExecuteTask(ctx, validatedInput)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	_, _ = w.Write(result)

}
