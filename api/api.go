package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"retl/db"
	"retl/inputs/types"
	k8sorhcestration "retl/k8s-orhcestration"
	"sync"

	"github.com/go-chi/chi"
	"github.com/google/uuid"
	"github.com/supabase-community/supabase-go"
)

func RegisterRoutes(router chi.Router) {

	dbClient, err := db.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	router.Use(corsMiddleware)
	router.Post("/input/create", func(w http.ResponseWriter, r *http.Request) {
		err := createInput(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Get("/pipeline-counts", func(w http.ResponseWriter, r *http.Request) {
		pipelineCounts, err := getPipelineCounts(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pipelineCounts)
	})

	router.Post("/output/create", func(w http.ResponseWriter, r *http.Request) {
		err := createOutput(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Get("/inputs", func(w http.ResponseWriter, r *http.Request) {
		inputs, err := getAllInputs(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")

		err = json.NewEncoder(w).Encode(inputs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Post("/pipeline/create", func(w http.ResponseWriter, r *http.Request) {
		err := createPipeline(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Get("/outputs", func(w http.ResponseWriter, r *http.Request) {
		inputs, err := getAllOutputs(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")

		err = json.NewEncoder(w).Encode(inputs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Post("/start", func(w http.ResponseWriter, r *http.Request) {
		err := startComponent(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Get("/pipelines", func(w http.ResponseWriter, r *http.Request) {
		pipelines, err := getAllPipelines(dbClient, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")

		err = json.NewEncoder(w).Encode(pipelines)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
}

type PipelineCount struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func getPipelineCounts(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) ([]PipelineCount, error) {
	pipelines, err := getAllPipelines(dbClient, w, r)
	if err != nil {
		return nil, err
	}

	var pipelineCounts []PipelineCount
	for _, pipeline := range pipelines {
		count := GetPipelineCount(pipeline.ID)
		pipelineCounts = append(pipelineCounts, PipelineCount{
			ID:    pipeline.ID,
			Name:  fmt.Sprintf("%s -> %s", pipeline.Source, pipeline.Destination),
			Count: count,
		})
	}

	return pipelineCounts, nil
}

var pipelineCounters = make(map[string]int)
var counterMutex sync.Mutex

func GetPipelineCount(pipelineName string) int {
	counterMutex.Lock()
	defer counterMutex.Unlock()
	return pipelineCounters[pipelineName]
}

type CPipelineRequest struct { 
	SourceID      string `json:"source_id"`       
	DestinationID string `json:"destination_id"`  
}

func corsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*") // Allow all origins
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
        
        if r.Method == http.MethodOptions {
            w.WriteHeader(http.StatusOK)
            return
        }
        
        next.ServeHTTP(w, r)
    })
}


func createPipeline(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) error {
	var reqBody CPipelineRequest

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return fmt.Errorf("error parsing request body: %v", err)
	}

	if reqBody.SourceID == "" || reqBody.DestinationID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return fmt.Errorf("missing required fields: pipeline_name, source_id, or destination_id")
	}

	insertBody := map[string]interface{}{
		"source":         reqBody.SourceID,
		"destination":    reqBody.DestinationID,
	}

	fb := dbClient.From("Pipelines").Insert(insertBody, true, "", "", "")
	result, count, err := fb.ExecuteString()
	if err != nil {
		log.Printf("Error inserting pipeline: %v", err)
		return fmt.Errorf("failed to create pipeline: %v", err)
	}

	log.Printf("Pipeline creation result: %s, Count: %d", result, count)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	return nil
}

type CreateRequest struct {
	ConnectorType string `json:"connector_type"` //input || output
	ConnectorName string `json:"connector_name"` //splunk || snowflake
	Name 		  string `json:"name"` //actual unique name
	Config *types.ConfigType `json:"config"`
}

var configStorageMap = make(map[string]*types.ConfigType)


type CreateParams struct {
	ID uuid.UUID `json:"id"`
	Name string `json:"name"`
	ConnectorName string `json:"connector_name"`
}


func createInput(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) error {
	fmt.Println("CREATE INPUT")
	s, err := json.Marshal(r.Body)
	fmt.Println(string(s))
	fmt.Println(err)
	var reqBody CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return err
	}
	id := uuid.New()
	insertBody := CreateParams{
		ID: id,
		Name: reqBody.Name,
		ConnectorName: reqBody.ConnectorName,
	}
	fmt.Println("INSERT BODY")
	fmt.Println(insertBody)
	fb := dbClient.From("Inputs").Insert(insertBody, true, "", "", "")
	result, count, err := fb.ExecuteString()
	if err != nil {
		log.Printf("Error executing insert: %v", err)
		return err
	}
	fmt.Println(result)
	configStorageMap[id.String()] = reqBody.Config
	log.Printf("Insert result: %s, Count: %d", result, count)
	fmt.Println(configStorageMap)
	return nil
}

type InputInDB struct {
	ID uuid.UUID `json:"id"`
	Name         string          `json:"name"`
	ConnectorName string         `json:"connector_name"`
}

type Input struct {
	ID string `json:"id"`
	Name         string          `json:"name"`
	ConnectorName string         `json:"connector_name"`
	ConnectorType string 		  `json:"connector_type"`
	Config 		  *types.ConfigType `json:"config"`
}

type OutputInDB struct {
	ID uuid.UUID `json:"id"`
	Name         string          `json:"name"`
	ConnectorName string         `json:"connector_name"`
}

type Output struct {
	ID string `json:"id"`
	Name         string          `json:"name"`
	ConnectorName string         `json:"connector_name"`
	ConnectorType string 		  `json:"connector_type"`
	Config 		  *types.ConfigType `json:"config"`
}

func getAllInputs(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) ([]Input, error) {
	var inputs []InputInDB
	fb := dbClient.From("Inputs").Select("*", "", false) 
	_, err := fb.ExecuteTo(&inputs)
	if err != nil {
		log.Printf("Error fetching inputs: %v", err)
		return nil, err
	}
	var inputsToReturn []Input
	for i := range inputs {
		val := configStorageMap[inputs[i].ID.String()]
		inputsToReturn = append(inputsToReturn, Input{
			ID: inputs[i].ID.String(),
			Name: inputs[i].Name,
			ConnectorName: inputs[i].ConnectorName,
			ConnectorType: "Input",
			Config: val,
		})	
	}
	return inputsToReturn, nil
}

func getAllOutputs(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) ([]Output, error) {
	var outputs []OutputInDB
	fb := dbClient.From("Outputs").Select("*", "", false) 
	_, err := fb.ExecuteTo(&outputs)
	if err != nil {
		log.Printf("Error fetching inputs: %v", err)
		return nil, err
	}
	var outputsToReturn []Output
	for i := range outputs {
		val := configStorageMap[outputs[i].ID.String()]
		outputsToReturn = append(outputsToReturn, Output{
			ID: outputs[i].ID.String(),
			Name: outputs[i].Name,
			ConnectorName: outputs[i].ConnectorName,
			ConnectorType: "Output",
			Config: val,
		})	
	}
	return outputsToReturn, nil
}

func createOutput(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) error {
	var reqBody CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return err
	}
	id := uuid.New()
	insertBody := CreateParams{
		ID: id,
		Name: reqBody.Name,
		ConnectorName: reqBody.ConnectorName,
	}
	fb := dbClient.From("Outputs").Insert(insertBody, true, "", "", "")
	result, count, err := fb.ExecuteString()
	if err != nil {
		log.Printf("Error executing insert: %v", err)
		return err
	}
	configStorageMap[id.String()] = reqBody.Config
	log.Printf("Insert result: %s, Count: %d", result, count)
	fmt.Println(configStorageMap)
	return nil
}

type CreatePipelineRequest struct {
	PipelineName string `json:"pipeline_name"`
	ConnectorType string `json:"connector_type"`
	ConnectorName string `json:"connector_name"`
	Config *types.ConfigType `json:"config"`
}

func startComponent(w http.ResponseWriter, r *http.Request) error {
	fmt.Println("STARTING COMPONENT")
	var reqBody CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		fmt.Println("BODY ERROR")
		w.WriteHeader(400)
		return err
	}
	fmt.Println(reqBody.ConnectorName)
	fmt.Println(reqBody.ConnectorType)
	fmt.Println(reqBody.Config)
	err := k8sorhcestration.RunOrchestration(reqBody.ConnectorType, reqBody.ConnectorName, reqBody.Config, reqBody.PipelineName)
	if err != nil {
		return err
	}
	return nil
}


type Pipeline struct {
	ID            string        `json:"id"`
	Source        string         `json:"source"`
	Destination   string       `json:"destination"`
}

func getAllPipelines(dbClient *supabase.Client, w http.ResponseWriter, r *http.Request) ([]Pipeline, error) {

	var pipelines []struct {
		ID           string `json:"id"`
		SourceID     string `json:"source"`
		DestinationID string `json:"destination"`
	}
	fb := dbClient.From("Pipelines").Select("*", "", false)
	_, err := fb.ExecuteTo(&pipelines)
	if err != nil {
		log.Printf("Error fetching pipelines: %v", err)
		return nil, err
	}

	var pipelinesToReturn []Pipeline
	for _, pipeline := range pipelines {
		_, sourceExists := configStorageMap[pipeline.SourceID]
		_, destExists := configStorageMap[pipeline.DestinationID]

		if sourceExists && destExists {
			pipelinesToReturn = append(pipelinesToReturn, Pipeline{
				ID:          pipeline.ID,
				Source:      pipeline.SourceID,
				Destination: pipeline.DestinationID,
			})
		} else {
			log.Printf("Warning: Input or Output not found for pipeline: %v", pipeline.ID)
		}
	}

	return pipelinesToReturn, nil
}