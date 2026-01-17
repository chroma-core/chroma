package chroma

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// ResultRow represents a single result item with all associated data.
// Use this with Rows() or At() for ergonomic iteration over results.
type ResultRow struct {
	ID        DocumentID
	Document  string           // Empty if not included in results
	Metadata  DocumentMetadata // nil if not included in results
	Embedding []float32        // nil if not included in results
	Score     float64          // Search: relevance score (higher=better); Query: distance (lower=better); Get: 0
}

type GetResult interface {
	// GetIDs returns the IDs of the documents in the result.
	GetIDs() DocumentIDs
	// GetDocuments returns the documents in the result.
	GetDocuments() Documents
	// GetMetadatas returns the metadatas of the documents in the result.
	GetMetadatas() DocumentMetadatas
	// GetEmbeddings returns the embeddings of the documents in the result.
	GetEmbeddings() embeddings.Embeddings
	// ToRecords converts the result to a Records object.
	ToRecords() Records
	// Count returns the number of documents in the result.
	Count() int
	// Next when using limint and offset, this will return the next page of results
	Next() (GetResult, error)
}

type GetResultImpl struct {
	Ids        DocumentIDs           `json:"ids,omitempty"`
	Documents  Documents             `json:"documents,omitempty"`
	Metadatas  DocumentMetadatas     `json:"metadatas,omitempty"`
	Embeddings embeddings.Embeddings `json:"embeddings,omitempty"`
	Include    []Include             `json:"include,omitempty"`
}

func (r *GetResultImpl) GetIDs() DocumentIDs {
	return r.Ids
}

func (r *GetResultImpl) GetDocuments() Documents {
	return r.Documents
}

func (r *GetResultImpl) GetMetadatas() DocumentMetadatas {
	return r.Metadatas
}

func (r *GetResultImpl) GetEmbeddings() embeddings.Embeddings {
	return r.Embeddings
}

func (r *GetResultImpl) ToRecords() Records {
	return nil
}

func (r *GetResultImpl) Count() int {
	return len(r.Ids)
}

func (r *GetResultImpl) Next() (GetResult, error) {
	return nil, errors.New("not implemented")
}

func (r *GetResultImpl) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var temp map[string]interface{}
	if err := decoder.Decode(&temp); err != nil {
		return errors.Wrap(err, "failed to unmarshal GetResult")
	}
	if _, ok := temp["ids"]; ok {
		r.Ids = make([]DocumentID, 0)
		if lst, ok := temp["ids"].([]interface{}); ok {
			for _, id := range lst {
				switch val := id.(type) {
				case string:
					r.Ids = append(r.Ids, DocumentID(val))
				default:
					return errors.Errorf("invalid id type: %T for %v", val, id)
				}
			}
		} else if lst != nil {
			return errors.Errorf("invalid ids: %v", temp["ids"])
		}
	}
	if _, ok := temp["documents"]; ok {
		r.Documents = make([]Document, 0)
		if lst, ok := temp["documents"].([]interface{}); ok {
			docs, err := NewTextDocumentsFromInterface(lst)
			if err != nil {
				return errors.Errorf("invalid documents: %v", err)
			}
			for _, d := range docs {
				doc := d
				r.Documents = append(r.Documents, &doc)
			}
		} else if lst != nil {
			return errors.Errorf("invalid documents: %v", temp["documents"])
		}
	}
	if _, ok := temp["metadatas"]; ok {
		r.Metadatas = make([]DocumentMetadata, 0)
		if lst, ok := temp["metadatas"].([]interface{}); ok {
			for _, metadata := range lst {
				if metadata == nil {
					r.Metadatas = append(r.Metadatas, nil)
					continue
				}
				switch val := metadata.(type) {
				case map[string]interface{}:
					metav, err := NewDocumentMetadataFromMap(val)
					if err != nil {
						return errors.Errorf("invalid metadata: %v", err)
					}
					r.Metadatas = append(r.Metadatas, metav)
				default:
					return errors.Errorf("invalid metadata type: %T for %v", val, metadata)
				}
			}
		} else if lst != nil {
			return errors.Errorf("invalid metadatas: %v", temp["metadatas"])
		}
	}
	if _, ok := temp["embeddings"]; ok {
		r.Embeddings = make([]embeddings.Embedding, 0)
		if lst, ok := temp["embeddings"].([]interface{}); ok {
			var err error
			r.Embeddings, err = embeddings.NewEmbeddingsFromInterface(lst)
			if err != nil {
				return errors.Errorf("invalid embeddings: %v", err)
			}
		} else if lst != nil {
			return errors.Errorf("invalid embeddings: %v", temp["embeddings"])
		}
	}
	if _, ok := temp["include"]; ok {
		r.Include = make([]Include, 0)
		if lst, ok := temp["include"].([]any); ok {
			for _, i := range lst {
				if v, ok := i.(string); ok {
					r.Include = append(r.Include, Include(v))
				} else {
					return errors.Errorf("invalid include type: %T for %v", i, lst)
				}
			}
		}
	}
	return nil
}

func (r *GetResultImpl) String() string {
	b, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return string(b)
}

// Rows returns all results as ResultRow slice for easy iteration.
func (r *GetResultImpl) Rows() []ResultRow {
	count := len(r.Ids)
	if count == 0 {
		return nil
	}
	rows := make([]ResultRow, count)
	for i := 0; i < count; i++ {
		rows[i] = r.buildRow(i)
	}
	return rows
}

// At returns the result at the given index with bounds checking.
// Returns false if index is out of bounds.
func (r *GetResultImpl) At(index int) (ResultRow, bool) {
	if index < 0 || index >= len(r.Ids) {
		return ResultRow{}, false
	}
	return r.buildRow(index), true
}

func (r *GetResultImpl) buildRow(i int) ResultRow {
	row := ResultRow{
		ID: r.Ids[i],
	}
	if i < len(r.Documents) && r.Documents[i] != nil {
		row.Document = r.Documents[i].ContentString()
	}
	if i < len(r.Metadatas) {
		row.Metadata = r.Metadatas[i]
	}
	if i < len(r.Embeddings) && r.Embeddings[i] != nil {
		row.Embedding = r.Embeddings[i].ContentAsFloat32()
	}
	return row
}

type QueryResult interface {
	GetIDGroups() []DocumentIDs
	GetDocumentsGroups() []Documents
	GetMetadatasGroups() []DocumentMetadatas
	GetEmbeddingsGroups() []embeddings.Embeddings
	GetDistancesGroups() []embeddings.Distances
	ToRecordsGroups() []Records
	CountGroups() int
}

type QueryResultImpl struct {
	IDLists         []DocumentIDs           `json:"ids"`
	DocumentsLists  []Documents             `json:"documents,omitempty"`
	MetadatasLists  []DocumentMetadatas     `json:"metadatas,omitempty"`
	EmbeddingsLists []embeddings.Embeddings `json:"embeddings,omitempty"`
	DistancesLists  []embeddings.Distances  `json:"distances,omitempty"`
	Include         []Include               `json:"include,omitempty"`
}

func (r *QueryResultImpl) GetIDGroups() []DocumentIDs {
	return r.IDLists
}

func (r *QueryResultImpl) GetDocumentsGroups() []Documents {
	return r.DocumentsLists
}

func (r *QueryResultImpl) GetMetadatasGroups() []DocumentMetadatas {
	return r.MetadatasLists
}

func (r *QueryResultImpl) GetEmbeddingsGroups() []embeddings.Embeddings {
	return r.EmbeddingsLists
}

func (r *QueryResultImpl) GetDistancesGroups() []embeddings.Distances {
	return r.DistancesLists
}

func (r *QueryResultImpl) ToRecordsGroups() []Records {
	return nil
}

func (r *QueryResultImpl) CountGroups() int {
	return len(r.IDLists)
}

func (r *QueryResultImpl) UnmarshalJSON(data []byte) error {
	var temp map[string]interface{}
	if err := json.Unmarshal(data, &temp); err != nil {
		return errors.Wrap(err, "failed to unmarshal QueryResult")
	}
	if _, ok := temp["ids"]; ok {
		r.IDLists = make([]DocumentIDs, 0)
		if lst, ok := temp["ids"].([]interface{}); ok {
			for _, id := range lst {
				switch val := id.(type) {
				case []interface{}:
					ids := make(DocumentIDs, 0)
					for _, id := range val {
						switch idVal := id.(type) {
						case string:
							ids = append(ids, DocumentID(idVal))
						default:
							return errors.Errorf("invalid id type: %T for %v", idVal, id)
						}
					}
					r.IDLists = append(r.IDLists, ids)
				default:
					return errors.Errorf("invalid ids: %v", temp["ids"])
				}
			}
		} else {
			return errors.Errorf("invalid ids: %v", temp["ids"])
		}
	}
	if _, ok := temp["documents"]; ok {
		r.DocumentsLists = make([]Documents, 0)
		if lst, ok := temp["documents"].([]interface{}); ok {
			innerDocList := make([]Document, 0)
			for _, docList := range lst {
				switch val := docList.(type) {
				case []interface{}:
					docs, err := NewTextDocumentsFromInterface(val)
					if err != nil {
						return errors.Errorf("invalid documents: %v", err)
					}
					for _, doc := range docs {
						document := doc
						innerDocList = append(innerDocList, &document)
					}
				default:
					return errors.Errorf("invalid documents: %v", temp["documents"])
				}
			}
			r.DocumentsLists = append(r.DocumentsLists, innerDocList)
		} else if lst != nil {
			return errors.Errorf("invalid documents: %v", temp["documents"])
		}
	}

	if _, ok := temp["metadatas"]; ok {
		r.MetadatasLists = make([]DocumentMetadatas, 0)
		if lst, ok := temp["metadatas"].([]interface{}); ok {
			for _, metadataList := range lst {
				switch val := metadataList.(type) {
				case []interface{}:
					metadata := make(DocumentMetadatas, 0)
					for _, metadataItem := range val {
						if metadataItem == nil {
							metadata = append(metadata, nil)
							continue
						}
						switch val := metadataItem.(type) {
						case map[string]interface{}:
							metav, err := NewDocumentMetadataFromMap(val)
							if err != nil {
								return errors.Errorf("invalid metadata: %v", err)
							}
							metadata = append(metadata, metav)
						default:
							return errors.Errorf("invalid metadata type: %T for %v", val, metadataItem)
						}
					}
					r.MetadatasLists = append(r.MetadatasLists, metadata)
				default:
					return errors.Errorf("invalid metadatas: %v", temp["metadatas"])
				}
			}
		} else if lst != nil {
			return errors.Errorf("invalid metadatas: %v", temp["metadatas"])
		}
	}

	if _, ok := temp["embeddings"]; ok {
		r.EmbeddingsLists = make([]embeddings.Embeddings, 0)
		if lst, ok := temp["embeddings"].([]interface{}); ok {
			for _, embeddingList := range lst {
				if embeddingList == nil {
					r.EmbeddingsLists = append(r.EmbeddingsLists, nil)
					continue
				}
				switch val := embeddingList.(type) {
				case []interface{}:
					emb, err := embeddings.NewEmbeddingsFromInterface(val)
					if err != nil {
						return errors.Errorf("invalid embeddings: %v", err)
					}
					r.EmbeddingsLists = append(r.EmbeddingsLists, emb)
				default:
					return errors.Errorf("invalid embeddings: %v", temp["embeddings"])
				}
			}
		} else if lst != nil {
			return errors.Errorf("invalid embeddings: %v", temp["embeddings"])
		}
	}
	if _, ok := temp["distances"]; ok {
		r.DistancesLists = make([]embeddings.Distances, 0)
		if lst, ok := temp["distances"].([]interface{}); ok {
			for _, distanceList := range lst {
				switch val := distanceList.(type) {
				case []interface{}:
					distances := make(embeddings.Distances, 0)
					for _, distanceItem := range val {
						switch val := distanceItem.(type) {
						case float64:
							distances = append(distances, embeddings.Distance(val))
						default:
							return errors.Errorf("invalid distance type: %T for %v", val, distanceItem)
						}
					}
					r.DistancesLists = append(r.DistancesLists, distances)
				default:
					return errors.Errorf("invalid distances: %v", temp["distances"])
				}
			}
		} else if lst != nil {
			return errors.Errorf("invalid distances: %v", temp["distances"])
		}
	}

	if _, ok := temp["include"]; ok {
		r.Include = make([]Include, 0)
		if lst, ok := temp["include"].([]any); ok {
			for _, i := range lst {
				if v, ok := i.(string); ok {
					r.Include = append(r.Include, Include(v))
				} else {
					return errors.Errorf("invalid include type: %T for %v", i, lst)
				}
			}
		}
	}
	return nil
}

func (r *QueryResultImpl) String() string {
	b, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return string(b)
}

// Rows returns the first query group's results for easy iteration.
// For multiple query groups, use RowGroups().
func (r *QueryResultImpl) Rows() []ResultRow {
	if len(r.IDLists) == 0 {
		return nil
	}
	return r.buildGroupRows(0)
}

// RowGroups returns all query groups as [][]ResultRow.
func (r *QueryResultImpl) RowGroups() [][]ResultRow {
	if len(r.IDLists) == 0 {
		return nil
	}
	groups := make([][]ResultRow, len(r.IDLists))
	for g := range r.IDLists {
		groups[g] = r.buildGroupRows(g)
	}
	return groups
}

// At returns the result at the given group and index with bounds checking.
// Returns false if either index is out of bounds.
func (r *QueryResultImpl) At(group, index int) (ResultRow, bool) {
	if group < 0 || group >= len(r.IDLists) {
		return ResultRow{}, false
	}
	ids := r.IDLists[group]
	if index < 0 || index >= len(ids) {
		return ResultRow{}, false
	}
	return r.buildRow(group, index), true
}

func (r *QueryResultImpl) buildGroupRows(g int) []ResultRow {
	ids := r.IDLists[g]
	if len(ids) == 0 {
		return nil
	}
	rows := make([]ResultRow, len(ids))
	for i := range ids {
		rows[i] = r.buildRow(g, i)
	}
	return rows
}

func (r *QueryResultImpl) buildRow(g, i int) ResultRow {
	row := ResultRow{
		ID: r.IDLists[g][i],
	}
	if g < len(r.DocumentsLists) && i < len(r.DocumentsLists[g]) && r.DocumentsLists[g][i] != nil {
		row.Document = r.DocumentsLists[g][i].ContentString()
	}
	if g < len(r.MetadatasLists) && i < len(r.MetadatasLists[g]) {
		row.Metadata = r.MetadatasLists[g][i]
	}
	if g < len(r.EmbeddingsLists) && i < len(r.EmbeddingsLists[g]) && r.EmbeddingsLists[g][i] != nil {
		row.Embedding = r.EmbeddingsLists[g][i].ContentAsFloat32()
	}
	if g < len(r.DistancesLists) && i < len(r.DistancesLists[g]) {
		row.Score = float64(r.DistancesLists[g][i])
	}
	return row
}
