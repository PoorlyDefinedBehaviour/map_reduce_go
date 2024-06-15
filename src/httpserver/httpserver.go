package httpserver

import (
	"fmt"
	"io"
	"net/http"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	v8 "rogchap.com/v8go"
)

// HTTP server used to receive new task requests from clients.
type HttpServer struct {
	mux *http.ServeMux
}

func New(master *master.Master) *HttpServer {
	srv := &HttpServer{
		mux: http.NewServeMux(),
	}

	srv.mux.HandleFunc("POST /task", srv.handleNewTask)

	return srv
}

func (srv *HttpServer) Start(addr string) error {
	if err := http.ListenAndServe(addr, srv.mux); err != nil {
		return fmt.Errorf("listening on addr: addr=%s %w", addr, err)
	}
	return nil
}

type NewTaskRequest struct {
	File                string `json:"file"`
	Folder              string `json:"folder"`
	NumberOfPartitions  uint16 `json:"numberOfPartitions"`
	NumberOfMapTasks    uint16 `json:"numberOfMapTasks"`
	NumberOfReduceTasks uint16 `json:"numberOfReduceTasks"`
}

func (srv *HttpServer) handleNewTask(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("\n\naaaaaaa err %+v\n\n", err)
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(500)
		return
	}
	fmt.Printf("\n\naaaaaaa string(body) %+v\n\n", string(body))

	v8Context := v8.NewContext()
	defer v8Context.Close()
	value, err := v8Context.RunScript(string(body), "handle_new_task.js")
	if err != nil {
		fmt.Printf("\n\naaaaaaa err %+v\n\n", err)
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(500)
		return
	}

	value, err = v8Context.RunScript("config", "handle_new_task.js")
	if err != nil {
		fmt.Printf("\n\naaaaaaa err %+v\n\n", err)
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(500)
		return
	}

	valueJsonBytes, err := value.MarshalJSON()
	if err != nil {
		fmt.Printf("\n\naaaaaaa err %+v\n\n", err)
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(500)
		return
	}
	fmt.Printf("\n\naaaaaaa string(valueJsonBytes) %+v err %+v\n\n", string(valueJsonBytes), err)
}
