//
//  ChromaDemoApp.swift
//  ChromaDemo
//
//  Created by Nicholas Arner on 5/14/25.
//

import SwiftUI
import Chroma

struct ContentView: View {
    @StateObject private var contentViewRef = ContentViewRef()
    @State private var docText: String = ""
    @State private var errorMessage: String? = nil
    @State private var logs: [String] = []
    @State private var docCounter: Int = 0
    @State private var showingSuccess: Bool = false
    @State private var collectionName: String = "my_collection"

    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    func resetState() {
        docText = ""
        errorMessage = nil
        logs.removeAll()
        docCounter = 0
        showingSuccess = false
        collectionName = "my_collection"
    }
    
    var body: some View {
        GeometryReader { geometry in
            VStack(spacing: 0) {
                if geometry.size.width > 600 {
                    // iPad/Mac layout (horizontal)
                    HStack(spacing: 0) {
                        mainContent
                            .frame(width: min(400, geometry.size.width * 0.4))
                        logsView
                    }
                } else {
                    // iPhone layout (vertical)
                    ScrollView {
                        VStack(spacing: 0) {
                            mainContent
                                .padding(.horizontal)
                            logsView
                        }
                    }
                }
            }
        }
        .environmentObject(contentViewRef)
        .onAppear {
            contentViewRef.addLog = addLog
        }
    }
    
    private var mainContent: some View {
        VStack(spacing: 24) {
            headerView
            databaseControls
            documentInput
        }
        .padding(.vertical)
    }
    
    private var headerView: some View {
        Text("Chroma Demo")
            .font(.title)
            .fontWeight(.bold)
            .multilineTextAlignment(.center)
            .padding(.top)
    }
    
    private var databaseControls: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Database Controls")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                TextField("Collection name", text: $collectionName)
                    .textFieldStyle(.roundedBorder)
                
                ActionButton(title: "Reset") {
                    resetState()  // Clear state first
                    try reset()
                    addLog("System reset complete")
                }
                
                ActionButton(title: "Initialize Ephemeral") {
                    try initialize()
                    addLog("Ephemeral Chroma initialized")
                    errorMessage = nil
                }
                
                ActionButton(title: "Create Collection") {
                    let collectionId = try createCollection(name: collectionName)
                    addLog("Ephemeral collection created: \(collectionId)")
                    errorMessage = nil
                }

                ActionButton(title: "Get All Documents") {
                    let res = try getAllDocuments(collectionName: collectionName)
                    let pairs = zip(res.ids, res.documents).map { id, doc in
                        "Document ID: \(id)\nContent: \(doc ?? "(nil)")"
                    }
                    addLog("--- Retrieved Documents ---")
                    pairs.forEach { addLog($0) }
                    addLog("--- End of Documents ---")
                    errorMessage = nil
                }
                
            }
            .padding()
        } label: {
            Label("Ephemeral Demo", systemImage: "database")
        }
    }
    
    private var documentInput: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Add Document")
                    .font(.headline)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                TextField("Enter document text...", text: $docText)
                    .textFieldStyle(.roundedBorder)
                
                ActionButton(title: "Add Document", disabled: docText.isEmpty) {
                    docCounter += 1
                    let ids = ["doc\(docCounter)"]
                    let embeddings: [[Float]] = [[0.1, 0.2, 0.3, 0.4]]
                    let docs = [docText]
                    _ = try addDocuments(
                        collectionName: collectionName,
                        ids: ids,
                        embeddings: embeddings,
                        documents: docs
                    )
                    showingSuccess = true
                    addLog("Document added to ephemeral: \(docText)")
                    docText = ""
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                        showingSuccess = false
                    }
                    errorMessage = nil
                }
            }
            .padding()
        } label: {
            Label("Document Input", systemImage: "doc.text")
        }
    }

    private var logsView: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Label("Activity Log", systemImage: "text.alignleft")
                    .font(.headline)
                Spacer()
            }
            .padding(.bottom, 4)
            
            ScrollView {
                VStack(alignment: .leading, spacing: 4) {
                    ForEach(logs.reversed(), id: \.self) { log in
                        Text(log)
                            .font(.system(.body, design: .monospaced))
                            .foregroundColor(.secondary)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .padding()
        .cornerRadius(10)
        .shadow(radius: 1)
    }
}

struct ActionButton: View {
    let title: String
    var disabled: Bool = false
    let action: () throws -> Void
    @EnvironmentObject private var contentViewRef: ContentViewRef
    
    var body: some View {
        Button(action: {
            do {
                try action()
            } catch {
                contentViewRef.addLog("Action failed: \(error)")
            }
        }) {
            Text(title)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
        }
        .buttonStyle(.borderedProminent)
        .disabled(disabled)
    }
}

class ContentViewRef: ObservableObject {
    var addLog: (String) -> Void = { _ in }
}
