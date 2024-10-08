package main

import (
	"net/http"
	"retl/api"

	"github.com/go-chi/chi"
)

func main() {
	// outputs.Start()
	r := chi.NewRouter()
	api.RegisterRoutes(r)
	http.ListenAndServe(":3001", r)
}

// package main

// import "retl/outputs"

// func main() {
// 	outputs.Start()
// 	// r := chi.NewRouter()
// 	// api.RegisterRoutes(r)
// 	// http.ListenAndServe(":3001", r)
// }



