package chunk

// Chunk represents a chunk of data
type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
}
