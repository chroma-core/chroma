//
//  DocumentInputsSectionView.swift
//  PersistentChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma


struct DocumentInputsSectionView: View {
    
    @Bindable var state: ChromaState
    
    @FocusState var focused: Bool
    
    var body: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Add Document (Persistent)")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                if state.collections.isEmpty {
                    Text("No persistent collections available")
                        .foregroundColor(.secondary)
                } else {
                    Picker("Collection", selection: $state.persistentCollectionName) {
                        ForEach(state.collections, id: \.self) { name in
                            Text(name).tag(name)
                        }
                    }
                    .pickerStyle(.menu)
                }
                
                TextField("Enter document text...", text: $state.docText)
                    .textFieldStyle(.roundedBorder)
                    .focused($focused)
                
                ActionButton(title: "Add Document", disabled: !state.isPersistentInitialized || state.docText.isEmpty || state.collections.isEmpty) {
                    self.focused = false
                    Task {
                        do {
                            state.docCounter += 1
                            let ids = ["persistent_doc\(state.docCounter)"]
                            let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                            let docs = [state.docText]
                            
                            _ = try await withCheckedThrowingContinuation { continuation in
                                DispatchQueue.global(qos: .userInitiated).async {
                                    do {
                                        let result = try addDocuments(
                                            collectionName: state.persistentCollectionName,
                                            ids: ids,
                                            embeddings: embeddings,
                                            documents: docs
                                        )
                                        continuation.resume(returning: result)
                                    } catch {
                                        continuation.resume(throwing: error)
                                    }
                                }
                            }
                            
                            await MainActor.run {
                                state.addLog("[Add] Document text: \(state.docText)")
                            }
                        } catch {
                            await MainActor.run {
                                state.addLog("Failed to add document: \(error)")
                            }
                        }
                    }
                }
            }
            .padding()
        } label: {
            Label("Document Input", systemImage: "doc.fill")
        }
    }
}
