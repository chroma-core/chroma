import SwiftUI
import Chroma

struct ContentView: View {
    @State private var status: String = ""
    @State private var result: String = ""
    @State private var docText: String = "Hello, Chroma!"
    @State private var errorMessage: String? = nil // For error reporting
    @State private var logs: [String] = []
    
    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    var body: some View {
        HStack(spacing: 0) {
            // Left side - Controls
            ScrollView {
                VStack(spacing: 20) {
                    Text("Chroma SwiftUI Demo")
                        .font(.title)
                    
                    // Ephemeral Demo
                    GroupBox("Ephemeral Demo") {
                        VStack(spacing: 15) {
                            Button("Initialize Ephemeral") {
                                do {
                                    try initialize()
                                    status = "Ephemeral Chroma initialized"
                                    addLog("Ephemeral Chroma initialized")
                                    errorMessage = nil
                                } catch {
                                    status = "Ephemeral init error"
                                    errorMessage = error.localizedDescription
                                    addLog("Ephemeral init error: \(error)")
                                }
                            }
                            
                            Button("Create Ephemeral Collection") {
                                do {
                                    let collectionId = try createCollection(name: "my_collection")
                                    status = "Ephemeral collection created: \(collectionId)"
                                    addLog("Ephemeral collection created: \(collectionId)")
                                    errorMessage = nil
                                } catch {
                                    status = "Ephemeral create error"
                                    errorMessage = error.localizedDescription
                                    addLog("Ephemeral create error: \(error)")
                                }
                            }
                            
                            VStack(spacing: 10) {
                                TextField("Document text", text: $docText)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                                
                                Button("Add Document (Ephemeral)") {
                                    do {
                                        let ids = ["doc1"]
                                        let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                                        let docs = [docText]
                                        _ = try addDocuments(
                                            collectionName: "my_collection",
                                            ids: ids,
                                            embeddings: embeddings,
                                            documents: docs
                                        )
                                        status = "Document added to ephemeral"
                                        addLog("Document added to ephemeral: \(docText)")
                                        errorMessage = nil
                                    } catch {
                                        status = "Ephemeral add error"
                                        errorMessage = error.localizedDescription
                                        addLog("Ephemeral add error: \(error)")
                                    }
                                }
                            }
                            
                            Button("Query Ephemeral") {
                                do {
                                    let queryEmbedding: [Float] = [0.1, 0.2, 0.3, 0.4]
                                    let res = try queryCollection(
                                        collectionName: "my_collection",
                                        queryEmbedding: queryEmbedding,
                                        nResults: 1
                                    )
                                    if let first = res.documents.first, let doc = first {
                                        result = "Found in ephemeral: \(doc)"
                                        addLog("Query found in ephemeral: \(doc)")
                                    } else {
                                        result = "(No document found in ephemeral)"
                                        addLog("No document found in ephemeral query")
                                    }
                                    errorMessage = nil
                                } catch {
                                    result = "Ephemeral query error"
                                    errorMessage = error.localizedDescription
                                    addLog("Ephemeral query error: \(error)")
                                }
                            }
                        }
                        .padding()
                    }
                    
                    // Persistent Demo
                    GroupBox("Persistent Demo") {
                        VStack(spacing: 15) {
                            Button("Initialize Persistent") {
                                do {
                                    try initializePersistent(dbPath: "chroma.db")
                                    status = "Persistent Chroma initialized"
                                    addLog("Persistent Chroma initialized")
                                    errorMessage = nil
                                } catch {
                                    status = "Persistent init error"
                                    errorMessage = error.localizedDescription
                                    addLog("Persistent init error: \(error)")
                                }
                            }
                            
                            Button("Create Persistent Collection") {
                                do {
                                    let collectionId = try createCollectionPersistent(name: "my_collection")
                                    status = "Persistent collection created: \(collectionId)"
                                    addLog("Persistent collection created: \(collectionId)")
                                    errorMessage = nil
                                } catch {
                                    status = "Persistent create error"
                                    errorMessage = error.localizedDescription
                                    addLog("Persistent create error: \(error)")
                                }
                            }
                            
                            VStack(spacing: 10) {
                                Button("Add Document (Persistent)") {
                                    do {
                                        let ids = ["doc1"]
                                        let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                                        let docs = [docText]
                                        _ = try addDocumentsPersistent(
                                            collectionName: "my_collection",
                                            ids: ids,
                                            embeddings: embeddings,
                                            documents: docs
                                        )
                                        status = "Document added to persistent"
                                        addLog("Document added to persistent: \(docText)")
                                        errorMessage = nil
                                    } catch {
                                        status = "Persistent add error"
                                        errorMessage = error.localizedDescription
                                        addLog("Persistent add error: \(error)")
                                    }
                                }
                            }
                            
                            Button("Query Persistent") {
                                do {
                                    let queryEmbedding: [Float] = [0.1, 0.2, 0.3, 0.4]
                                    let res = try queryCollectionPersistent(
                                        collectionName: "my_collection",
                                        queryEmbedding: queryEmbedding,
                                        nResults: 1
                                    )
                                    if let first = res.documents.first, let doc = first {
                                        result = "Found in persistent: \(doc)"
                                        addLog("Query found in persistent: \(doc)")
                                    } else {
                                        result = "(No document found in persistent)"
                                        addLog("No document found in persistent query")
                                    }
                                    errorMessage = nil
                                } catch {
                                    result = "Persistent query error"
                                    errorMessage = error.localizedDescription
                                    addLog("Persistent query error: \(error)")
                                }
                            }
                        }
                        .padding()
                    }
                    
                    Text(status)
                        .foregroundColor(.blue)
                    Text(result)
                        .foregroundColor(.green)
                    
                    if let errorMessage = errorMessage {
                        Text("Error: \(errorMessage)")
                            .foregroundColor(.red)
                            .padding()
                    }
                }
                .padding()
            }
            .frame(width: 400)
            
            // Right side - Logs
            ScrollView {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Logs")
                        .font(.headline)
                        .padding(.bottom)
                    
                    ForEach(logs.reversed(), id: \.self) { log in
                        Text(log)
                            .font(.system(.body, design: .monospaced))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding()
            }
        }
    }
}
