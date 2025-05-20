import SwiftUI
import Chroma

struct EphemeralDemoView: View {
    @Binding var collectionName: String
    @Binding var collections: [String]
    @Binding var isInitialized: Bool
    @Binding var docText: String
    @Binding var docCounter: Int
    @Binding var showingSuccess: Bool
    @State private var queryEmbeddingText: String = "0.1,0.2,0.3,0.4"
    @State private var includeFieldsText: String = "documents"
    var refreshCollections: () -> Void
    var addLog: (String) -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            GroupBox {
                VStack(spacing: 16) {
                    Text("Ephemeral Database Controls")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
          
                    TextField("Collection name", text: $collectionName)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Create Collection", disabled: !isInitialized) {
                        let collectionId = try createCollection(name: collectionName)
                        addLog("Ephemeral collection created: \(collectionId)")
                        refreshCollections()
                    }
                    
                    ActionButton(title: "List Collections", disabled: !isInitialized) {
                        refreshCollections()
                    }

                    ActionButton(title: "Get All Documents", disabled: !isInitialized || collections.isEmpty) {
                         if collections.isEmpty {
                             addLog("[GetAll] No collections to get documents from")
                             return
                         }
                         addLog("[GetAll] --- Retrieved Documents ---")
                         Task {
                             for collection in collections {
                                 addLog("[GetAll] Fetching documents for collection: \(collection)")
                                 do {
                                     let res = try await withCheckedThrowingContinuation { continuation in
                                         DispatchQueue.global(qos: .userInitiated).async {
                                             do {
                                                 let res = try getAllDocuments(collectionName: collection)
                                                 continuation.resume(returning: res)
                                             } catch {
                                                 continuation.resume(throwing: error)
                                             }
                                         }
                                     }
                                     addLog("[GetAll] Got \(res.ids.count) documents for collection: \(collection)")
                                     if res.ids.isEmpty {
                                         addLog("[GetAll]   (empty)")
                                     } else {
                                         let pairs = zip(res.ids, res.documents).map { id, doc in
                                             "[GetAll]   Document ID: \(id)\n  Content: \(doc ?? "(nil)")"
                                         }
                                         pairs.forEach { addLog($0) }
                                     }
                                 } catch {
                                     addLog("[GetAll] Error fetching documents for collection \(collection): \(error)")
                                 }
                             }
                             addLog("[GetAll] --- End of Documents ---")
                         }
                     }
                    
                    ActionButton(title: "Reset Chroma") {
                        try reset()
                        addLog("System reset complete")
                    }
                    
                    Text("Data only persists while app is running")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                .padding()
            } label: {
                Label("Ephemeral Demo", systemImage: "cylinder.split")
            }
            
            GroupBox {
                VStack(spacing: 16) {
                    Text("Add Document (Ephemeral)")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    
                    if collections.isEmpty {
                        Text("No collections available")
                            .foregroundColor(.secondary)
                    } else {
                        Picker("Collection", selection: $collectionName) {
                            ForEach(collections, id: \.self) { name in
                                Text(name).tag(name)
                            }
                        }
                        .pickerStyle(.menu)
                    }
                    
                    TextField("Enter document text...", text: $docText)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Add Document", disabled: !isInitialized || docText.isEmpty || collections.isEmpty) {
                        docCounter += 1
                        let ids = ["doc\(docCounter)"]
                        let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                        let docs = [docText]
                        addLog("[Add] Attempting to add document to collection: \(collectionName)")
                        addLog("[Add] Document text: \(docText)")
                        addLog("[Add] Embedding: \(embeddings[0]) (dim: \(embeddings[0].count))")
                        _ = try addDocuments(
                            collectionName: collectionName,
                            ids: ids,
                            embeddings: embeddings,
                            documents: docs
                        )
                        showingSuccess = true
                        addLog("[Add] Document added to ephemeral collection '\(collectionName)': \(docText)")
                        docText = ""
                        DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                            showingSuccess = false
                        }
                    }
                }
                .padding()
            } label: {
                Label("Ephemeral Document Input", systemImage: "doc.text")
            }
            // --- Query Section ---
            GroupBox {
                VStack(spacing: 16) {
                    Text("Query Collection (Ephemeral)")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    
                    if collections.isEmpty {
                        Text("No collections available to query")
                            .foregroundColor(.secondary)
                    } else {
                        Picker("Collection", selection: $collectionName) {
                            ForEach(collections, id: \.self) { name in
                                Text(name).tag(name)
                            }
                        }
                        .pickerStyle(.menu)
                    }
                    
                    // Query Embedding Input
                    TextField("Enter query embedding (comma-separated floats)", text: $queryEmbeddingText)
                        .textFieldStyle(.roundedBorder)

                    // Include fields input
                    TextField("Fields to include (comma-separated, e.g. documents,embeddings)", text: $includeFieldsText)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Query Collection", disabled: !isInitialized || collections.isEmpty) {
                        guard let embedding = parseEmbedding(queryEmbeddingText) else {
                            addLog("[Query] Invalid embedding format. Please enter comma-separated floats.")
                            return
                        }
                        let includeFields = includeFieldsText.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                        addLog("[Query] Querying collection: \(collectionName)")
                        addLog("[Query] Embedding: \(embedding) (dim: \(embedding.count))")
                        addLog("[Query] Include fields: \(includeFields)")
                        Task {
                            do {
                                let nResults: UInt32 = 5
                                let result = try await withCheckedThrowingContinuation { continuation in
                                    DispatchQueue.global(qos: .userInitiated).async {
                                        do {
                                            let result = try queryCollection(
                                                collectionName: collectionName,
                                                queryEmbeddings: [embedding], // batched
                                                nResults: nResults,
                                                whereFilter: nil,
                                                ids: nil,
                                                include: includeFields
                                            )
                                            continuation.resume(returning: result)
                                        } catch {
                                            continuation.resume(throwing: error)
                                        }
                                    }
                                }
                                let ids = result.ids.first ?? []
                                let docs = result.documents.first ?? []
                                await MainActor.run {
                                    addLog("[Query] Query result IDs: \(ids)")
                                    addLog("[Query] Query result Docs: \(docs)")
                                    addLog("[Query] Raw QueryResult: \(result)")
                                }
                            } catch {
                                await MainActor.run {
                                    addLog("[Query] Query failed: \(error)")
                                }
                            }
                        }
                    }
                }
                .padding()
            } label: {
                Label("Ephemeral Query", systemImage: "magnifyingglass")
            }
        }
    }
}

// Helper to parse comma-separated floats
private func parseEmbedding(_ text: String) -> [Float]? {
    let parts = text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
    let floats = parts.compactMap { Float($0) }
    return floats.count == parts.count ? floats : nil
}
