//
//  DocumentInputSectionView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

struct DocumentInputSectionView: View {
    
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
    
    var docCounter: Int {
        state.docCounter
    }
    
    @FocusState var focused: Bool
    
    var body: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Add Document")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                if collections.isEmpty {
                    Text("No collections available")
                        .foregroundColor(.secondary)
                } else {
                    Picker("Collection", selection: $state.collectionName) {
                        ForEach(collections, id: \.self) { name in
                            Text(name).tag(name)
                        }
                    }
                    .pickerStyle(.menu)
                }
                
                TextField("Enter document text...", text: $state.docText)
                    .textFieldStyle(.roundedBorder)
                    .focused($focused)
                
                ActionButton(title: "Add Document",
                             disabled: !isInitialized || docText.isEmpty || collections.isEmpty) {
                    self.focused = false
                    state.docCounter += 1
                    let ids = ["doc\(docCounter)"]
                    let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                    let docs = [docText]
                    state.addLog("[Add] Attempting to add document to collection: \(collectionName)")
                    state.addLog("[Add] Document text: \(docText)")
                    state.addLog("[Add] Embedding: \(embeddings[0]) (dim: \(embeddings[0].count))")
                    _ = try addDocuments(
                        collectionName: collectionName,
                        ids: ids,
                        embeddings: embeddings,
                        documents: docs
                    )
                    state.addLog("[Add] Document added to collection '\(collectionName)': \(docText)")
                    state.docText = ""
                }
            }
            .padding()
        } label: {
            Label("Document Input", systemImage: "doc.text")
        }
    }
}
