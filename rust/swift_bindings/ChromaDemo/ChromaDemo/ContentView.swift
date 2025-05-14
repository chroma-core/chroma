//
//  ChromaDemoApp.swift
//  ChromaDemo
//
//  Created by Nicholas Arner on 5/14/25.
//


import SwiftUI
import Chroma

struct ContentView: View {
    @State private var status: String = ""
    @State private var result: String = ""
    @State private var docText: String = "Hello, Chroma!"
    @State private var errorMessage: String? = nil 
    @State private var logs: [String] = []
    
    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width > 600 {
                // iPad/Mac layout (horizontal)
                HStack(spacing: 0) {
                    controlsView
                        .frame(width: min(400, geometry.size.width * 0.4))
                    logsView
                }
            } else {
                // iPhone layout (vertical)
                ScrollView {
                    VStack(spacing: 0) {
                        controlsView
                            .padding(.horizontal)
                        logsView
                    }
                }
            }
        }
    }
    
    private var controlsView: some View {
        VStack(spacing: 20) {
            Text("Chroma SwiftUI Demo")
                .font(.title)
                .multilineTextAlignment(.center)
            
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
                    .frame(maxWidth: .infinity)
                    
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
                    .frame(maxWidth: .infinity)
                    
                    VStack(spacing: 10) {
                        TextField("Document text", text: $docText)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                        
                        Button("Add Document (Ephemeral)") {
                            do {
                                let ids = [UUID().uuidString]
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
                        .frame(maxWidth: .infinity)
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
                    .frame(maxWidth: .infinity)
                }
                .padding()
            }
            
            Text(status)
                .foregroundColor(.blue)
                .multilineTextAlignment(.center)
            
            Text(result)
                .foregroundColor(.green)
                .multilineTextAlignment(.center)
            
            if let errorMessage = errorMessage {
                Text("Error: \(errorMessage)")
                    .foregroundColor(.red)
                    .padding()
                    .multilineTextAlignment(.center)
            }
        }
        .padding(.vertical)
    }
    
    private var logsView: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Logs")
                .font(.headline)
                .padding(.bottom)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(logs.reversed(), id: \.self) { log in
                        Text(log)
                            .font(.system(.body, design: .monospaced))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .padding()
    }
}
