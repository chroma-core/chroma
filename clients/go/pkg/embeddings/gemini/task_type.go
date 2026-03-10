package gemini

// TaskType defines the Gemini embedding task intent.
type TaskType string

const (
	TaskTypeSemanticSimilarity TaskType = "SEMANTIC_SIMILARITY"
	TaskTypeClassification     TaskType = "CLASSIFICATION"
	TaskTypeClustering         TaskType = "CLUSTERING"
	TaskTypeRetrievalDocument  TaskType = "RETRIEVAL_DOCUMENT"
	TaskTypeRetrievalQuery     TaskType = "RETRIEVAL_QUERY"
	TaskTypeCodeRetrievalQuery TaskType = "CODE_RETRIEVAL_QUERY"
	TaskTypeQuestionAnswering  TaskType = "QUESTION_ANSWERING"
	TaskTypeFactVerification   TaskType = "FACT_VERIFICATION"
)

// IsValid returns true when the task type is one of the documented Gemini embedding task types.
func (t TaskType) IsValid() bool {
	switch t {
	case TaskTypeSemanticSimilarity,
		TaskTypeClassification,
		TaskTypeClustering,
		TaskTypeRetrievalDocument,
		TaskTypeRetrievalQuery,
		TaskTypeCodeRetrievalQuery,
		TaskTypeQuestionAnswering,
		TaskTypeFactVerification:
		return true
	default:
		return false
	}
}
