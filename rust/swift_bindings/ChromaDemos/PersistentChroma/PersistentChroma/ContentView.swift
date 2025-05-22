//
//  ContentView.swift
//  PersistentChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

#if canImport(UIKit)
import UIKit
extension View {
    func dismissKeyboard() {
        UIApplication.shared.sendAction(#selector(UIResponder.resignFirstResponder), to: nil, from: nil, for: nil)
    }
}
#else
extension View {
    func dismissKeyboard() {
        // No-op for macOS
    }
}
#endif

// Helper for iOS platform check
func isIPad() -> Bool {
    #if canImport(UIKit)
    return UIDevice.current.userInterfaceIdiom == .pad
    #else
    return false
    #endif
}

func isIPhone() -> Bool {
    #if canImport(UIKit)
    return UIDevice.current.userInterfaceIdiom == .phone
    #else
    return false
    #endif
}

struct ContentView: View {
    
    @State var state: ChromaState = .init()
    @Environment(\.horizontalSizeClass) var horizontalSizeClass
    
    var body: some View {
        GeometryReader { geometry in
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
                ZStack {
                    VStack(spacing: 0) {
                        ScrollView {
                            VStack(spacing: 24) {
                                headerView
                                databaseControls
                                documentInputSection
                                querySection
                            }
                            .padding(.horizontal)
                            .padding(.bottom, geometry.size.height * 0.3)
                        }
                    }
                    
                    VStack {
                        Spacer()
                        if isIPad() {
                            logsView
                        } else {
                            logsView
                                .frame(height: geometry.size.height * 0.3)
                                .background(Color(uiColor: .systemBackground))
                        }
                    }
                }
            }
        }
        .onAppear {
            state.checkForPersistentData()
            do {
                if !state.isPersistentInitialized {
                    let fileManager = FileManager.default
                    if !fileManager.fileExists(atPath: state.persistentPath) {
                        try fileManager.createDirectory(atPath: state.persistentPath, withIntermediateDirectories: true)
                        state.addLog("Created persistent directory at: \(state.persistentPath)")
                    }
                    state.addLog("Using persistent storage at: \(state.persistentPath)")
                    try initializeWithPath(path: state.persistentPath, allowReset: true)
                    state.isPersistentInitialized = true
                    state.refreshCollections()
                }
            } catch {
                state.addLog("Failed to initialize: \(error)")
            }
        }
    }
    
    private var headerView: some View {
        VStack(spacing: 8) {
            Text("Persistent Chroma Demo")
                .font(.title)
                .bold()
        }
        .frame(maxWidth: .infinity)
        .padding()
    }
    
    private var databaseControls: some View {
        DatabaseControlsView(state: state)
    }
    
    private var documentInputSection: some View {
       DocumentInputsSectionView(state: state)
    }
    
    // --- Persistent Query Section ---
    private var querySection: some View {
        QuerySectionView(state: state)
    }
    
    private var logsView: some View {
        LogsView(state: state)
    }
}

// Helper to parse comma-separated floats
func parseEmbedding(_ text: String) -> [Float]? {
    let parts = text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
    let values = parts.compactMap { Float($0) }
    guard values.count == parts.count, !values.isEmpty else { return nil }
    return values
}
