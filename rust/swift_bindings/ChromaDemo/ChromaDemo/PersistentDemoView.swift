import SwiftUI
import Chroma

struct PersistentDemoView: View {
    @Binding var persistentCollectionName: String
    @Binding var collections: [String]
    @Binding var isPersistentInitialized: Bool
    @Binding var docText: String
    @Binding var docCounter: Int
    @Binding var showingSuccess: Bool
    var refreshCollections: () -> Void
    @Binding var persistentPath: String
    var addLog: (String) -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            GroupBox {
                VStack(spacing: 16) {
                    Text("Persistent Database Controls")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Storage path:")
                            .font(.caption)
                        
                        HStack {
                            Text(persistentPath)
                                .font(.caption)
                                .foregroundColor(.secondary)
                                .lineLimit(1)
                                .truncationMode(.middle)
                        }
                    }
                          
                    TextField("Collection name", text: $persistentCollectionName)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Create Collection", disabled: !isPersistentInitialized) {
                        let collectionId = try createCollection(name: persistentCollectionName)
                        addLog("Persistent collection created: \(collectionId)")
                        refreshCollections()
                    }
                    
                    ActionButton(title: "List Collections", disabled: !isPersistentInitialized) {
                        refreshCollections()
                    }

                    ActionButton(title: "Get All Documents", disabled: !isPersistentInitialized || collections.isEmpty) {
                        if collections.isEmpty {
                            addLog("No persistent collections to get documents from")
                            return
                        }
                        
                        addLog("--- Retrieved Persistent Documents ---")
                        for collection in collections {
                            addLog("Collection: \(collection)")
                            let res = try getAllDocuments(collectionName: collection)
                            if res.ids.isEmpty {
                                addLog("  (empty)")
                            } else {
                                let pairs = zip(res.ids, res.documents).map { id, doc in
                                    "  Document ID: \(id)\n  Content: \(doc ?? "(nil)")"
                                }
                                pairs.forEach { addLog($0) }
                            }
                        }
                        addLog("--- End of Documents ---")
                    }
                    
                    Button {
                        addLog("Storage location at: \(persistentPath)")
                    } label: {
                        Text("Show Storage Location")
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 8)
                    }
                    .background(Color.gray.opacity(0.2))
                    .cornerRadius(8)
                    .disabled(!isPersistentInitialized)
                    
                    Text("Data will persist between app launches")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                .padding()
            } label: {
                Label("Persistent Demo", systemImage: "externaldrive.fill")
            }
            
            GroupBox {
                VStack(spacing: 16) {
                    Text("Add Document (Persistent)")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    
                    if collections.isEmpty {
                        Text("No persistent collections available")
                            .foregroundColor(.secondary)
                    } else {
                        Picker("Collection", selection: $persistentCollectionName) {
                            ForEach(collections, id: \.self) { name in
                                Text(name).tag(name)
                            }
                        }
                        .pickerStyle(.menu)
                    }
                    
                    TextField("Enter document text...", text: $docText)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Add Document", disabled: !isPersistentInitialized || docText.isEmpty || collections.isEmpty) {
                        docCounter += 1
                        let ids = ["persistent_doc\(docCounter)"]
                        let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                        let docs = [docText]
                        _ = try addDocuments(
                            collectionName: persistentCollectionName,
                            ids: ids,
                            embeddings: embeddings,
                            documents: docs
                        )
                        showingSuccess = true
                        addLog("Document added to persistent collection '\(persistentCollectionName)': \(docText)")
                        docText = ""
                        DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                            showingSuccess = false
                        }
                    }
                }
                .padding()
            } label: {
                Label("Persistent Document Input", systemImage: "doc.fill")
            }
            
            // --- Persistent Query Section ---
            GroupBox {
                VStack(spacing: 16) {
                    Text("Query Collection (Persistent)")
                        .font(.headline)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    
                    if collections.isEmpty {
                        Text("No persistent collections available to query")
                            .foregroundColor(.secondary)
                    } else {
                        Picker("Collection", selection: $persistentCollectionName) {
                            ForEach(collections, id: \.self) { name in
                                Text(name).tag(name)
                            }
                        }
                        .pickerStyle(.menu)
                    }
                    
                    // Query Embedding Input
                    @State var persistentQueryEmbeddingText: String = "0.1,0.2,0.3,0.4"
                    TextField("Enter query embedding (comma-separated floats)", text: $persistentQueryEmbeddingText)
                        .textFieldStyle(.roundedBorder)

                    // Include fields input
                    @State var persistentIncludeFieldsText: String = "documents"
                    TextField("Fields to include (comma-separated, e.g. documents,embeddings)", text: $persistentIncludeFieldsText)
                        .textFieldStyle(.roundedBorder)
                    
                    ActionButton(title: "Query Collection", disabled: !isPersistentInitialized || collections.isEmpty) {
                        guard let embedding = parseEmbedding(persistentQueryEmbeddingText) else {
                            addLog("[Query] Invalid embedding format. Please enter comma-separated floats.")
                            return
                        }
                        let includeFields = persistentIncludeFieldsText.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                        addLog("[Query] Querying persistent collection: \(persistentCollectionName)")
                        addLog("[Query] Embedding: \(embedding) (dim: \(embedding.count))")
                        addLog("[Query] Include fields: \(includeFields)")
                        Task {
                            do {
                                let nResults: UInt32 = 5
                                let result = try await withCheckedThrowingContinuation { continuation in
                                    DispatchQueue.global(qos: .userInitiated).async {
                                        do {
                                            let result = try queryCollection(
                                                collectionName: persistentCollectionName,
                                                queryEmbeddings: [embedding],
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
                Label("Persistent Query", systemImage: "magnifyingglass")
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
