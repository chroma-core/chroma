//
//  QuerySectionView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma


struct QuerySectionView: View {
    
    @Bindable var state: ChromaState
    
    var collections: [String] {
        state.collections
    }
    
    var collectionName: String {
        state.collectionName
    }
    
    var docText: String {
        state.docText
    }
    
    var isInitialized: Bool {
        state.isInitialized
    }
    
    @FocusState var focused: Bool
    @FocusState var focused2: Bool
    
    var body: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Query Collection")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                if collections.isEmpty {
                    Text("No collections available to query")
                        .foregroundColor(.secondary)
                } else {
                    Picker("Collection", selection: $state.collectionName) {
                        ForEach(collections, id: \.self) { name in
                            Text(name).tag(name)
                        }
                    }
                    .pickerStyle(.menu)
                }
                
                // Query Embedding Input
                TextField("Enter query embedding (comma-separated floats)", text: $state.queryEmbeddingText)
                    .textFieldStyle(.roundedBorder)
                    .focused($focused)

                // Include fields input
                TextField("Fields to include (comma-separated, e.g. documents,embeddings)", text: $state.includeFieldsText)
                    .textFieldStyle(.roundedBorder)
                    .focused($focused2)
                
                ActionButton(title: "Query Collection",
                             disabled: !isInitialized || collections.isEmpty) {
                    self.focused = false
                    self.focused2 = false
                    guard let embedding = parseEmbedding(state.queryEmbeddingText) else {
                        state.addLog("[Query] Invalid embedding format. Please enter comma-separated floats.")
                        return
                    }
                    let includeFields = state.includeFieldsText.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                    state.addLog("[Query] Querying collection: \(collectionName)")
                    state.addLog("[Query] Embedding: \(embedding) (dim: \(embedding.count))")
                    state.addLog("[Query] Include fields: \(includeFields)")
                    
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
                                state.addLog("[Query] Query result IDs: \(ids)")
                                state.addLog("[Query] Query result Docs: \(docs)")
                                state.addLog("[Query] Raw QueryResult: \(result)")
                            }
                        } catch {
                            await MainActor.run {
                                state.addLog("[Query] Query failed: \(error)")
                            }
                        }
                    }
                }
            }
            .padding()
        } label: {
            Label("Query", systemImage: "magnifyingglass")
        }
    }
}

