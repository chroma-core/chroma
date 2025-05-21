//
//  ContentView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

class ContentViewRef: ObservableObject {
    var addLog: (String) -> Void = { _ in }
}

struct ContentView: View {
    @StateObject private var contentViewRef = ContentViewRef()
    @State private var docText: String = ""
    @State private var errorMessage: String? = nil
    @State private var logs: [String] = []
    @State private var docCounter: Int = 0
    @State private var showingSuccess: Bool = false
    @State private var collectionName: String = "my_collection"
    @State private var collections: [String] = []
    @State private var isInitialized: Bool = false
    @State private var queryEmbeddingText: String = "0.1,0.2,0.3,0.4"
    @State private var includeFieldsText: String = "documents"
    
    func addLog(_ message: String) {
        logs.append("[\(Date().formatted(date: .omitted, time: .standard))] \(message)")
    }
    
    func refreshCollections() {
        guard isInitialized else {
            addLog("Cannot refresh collections: Chroma not initialized")
            return
        }
        
        do {
            collections = try listCollections()
            addLog("Found \(collections.count) collections")
        } catch {
            addLog("Failed to list collections: \(error)")
        }
    }
    
    func initialize(allowReset: Bool = true) throws {
        try Chroma.initialize(allowReset: allowReset)
        
        isInitialized = true
        addLog("Ephemeral Chroma initialized (allowReset: \(allowReset))")
    }
    
    func reset() throws {
        try Chroma.reset()
        collections = []
        isInitialized = false
        addLog("Chroma reset complete")
    }
    
    var body: some View {
        GeometryReader { geometry in
            VStack(spacing: 0) {
                if geometry.size.width > 600 {
                    HStack(spacing: 0) {
                        ScrollView {
                            VStack(spacing: 24) {
                                headerView
                                databaseControls
                                documentInputSection
                                querySection
                            }
                            .padding(.vertical)
                        }
                        .frame(width: min(500, geometry.size.width * 0.5))
                        logsView
                    }
                } else {
                    ScrollView {
                        VStack(spacing: 0) {
                            VStack(spacing: 24) {
                                headerView
                                databaseControls
                                documentInputSection
                                querySection
                            }
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
            do {
                if !isInitialized {
                    try initialize()
                    refreshCollections()
                }
            } catch {
                addLog("Failed to initialize: \(error)")
            }
        }
        .overlay {
            if showingSuccess {
                SuccessToast()
                    .transition(.move(edge: .top).combined(with: .opacity))
            }
        }
        .animation(.easeInOut, value: showingSuccess)
    }
    
    private var headerView: some View {
        VStack(spacing: 8) {
            Text("Ephemeral Chroma Demo")
                .font(.title)
                .bold()
            
            Text("In-memory vector database for semantic search")
                .font(.subheadline)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding()
    }
    
    private var databaseControls: some View {
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
            Label("Ephemeral Demo", systemImage: "cylinder.split.1x2")
        }
    }
    
    private var documentInputSection: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Add Document")
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
                    addLog("[Add] Document added to collection '\(collectionName)': \(docText)")
                    docText = ""
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                        showingSuccess = false
                    }
                }
            }
            .padding()
        } label: {
            Label("Document Input", systemImage: "doc.text")
        }
    }
    
    private var querySection: some View {
        GroupBox {
            VStack(spacing: 16) {
                Text("Query Collection")
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
            Label("Query", systemImage: "magnifyingglass")
        }
    }
    
    private var logsView: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("Activity Log")
                    .font(.headline)
                
                Spacer()
                
                Button {
                    logs = []
                } label: {
                    Label("Clear", systemImage: "trash")
                        .labelStyle(.iconOnly)
                }
                .buttonStyle(.borderless)
            }
            .padding(.horizontal)
            .padding(.top)
            
            Divider()
            
            if logs.isEmpty {
                Text("No activity yet")
                    .foregroundColor(.secondary)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding()
            } else {
                ScrollViewReader { scrollView in
                    ScrollView {
                        VStack(alignment: .leading, spacing: 8) {
                            ForEach(logs.indices, id: \.self) { index in
                                Text(logs[index])
                                    .font(.system(.caption, design: .monospaced))
                                    .id(index)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 4)
                                    .background(index % 2 == 0 ? Color.clear : Color.gray.opacity(0.05))
                            }
                        }
                        .onChange(of: logs.count) { _, _ in
                            if let lastIndex = logs.indices.last {
                                scrollView.scrollTo(lastIndex, anchor: .bottom)
                            }
                        }
                    }
                }
            }
        }
    }
}

// Helper to parse comma-separated floats
private func parseEmbedding(_ text: String) -> [Float]? {
    let parts = text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
    let values = parts.compactMap { Float($0) }
    guard values.count == parts.count, !values.isEmpty else { return nil }
    return values
}

// Simple action button with manual styling
struct ActionButton: View {
    let title: String
    var disabled: Bool = false
    var action: () throws -> Void
    @State private var inProgress = false
    @State private var errorMessage: String? = nil
    
    var body: some View {
        Button {
            inProgress = true
            errorMessage = nil
            
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try action()
                    DispatchQueue.main.async {
                        inProgress = false
                    }
                } catch {
                    DispatchQueue.main.async {
                        errorMessage = error.localizedDescription
                        inProgress = false
                    }
                }
            }
        } label: {
            HStack {
                if inProgress {
                    ProgressView()
                        .controlSize(.small)
                        .frame(width: 16, height: 16)
                }
                
                Text(title)
                    .frame(maxWidth: .infinity)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 8)
        }
        .buttonStyle(.bordered)
        .disabled(disabled || inProgress)
        .alert("Operation Failed", isPresented: Binding<Bool>(
            get: { errorMessage != nil },
            set: { if !$0 { errorMessage = nil } }
        )) {
            Button("OK", role: .cancel) { errorMessage = nil }
        } message: {
            if let errorMessage = errorMessage {
                Text(errorMessage)
            }
        }
    }
}

// Toast shown when an operation succeeds
struct SuccessToast: View {
    var body: some View {
        HStack {
            Image(systemName: "checkmark.circle.fill")
                .foregroundColor(.green)
            Text("Success!")
                .foregroundColor(.white)
                .font(.subheadline)
                .bold()
        }
        .padding(.horizontal, 20)
        .padding(.vertical, 10)
        .background(
            RoundedRectangle(cornerRadius: 30)
                .fill(Color.black.opacity(0.8))
        )
        .padding(.top, 20)
    }
}
