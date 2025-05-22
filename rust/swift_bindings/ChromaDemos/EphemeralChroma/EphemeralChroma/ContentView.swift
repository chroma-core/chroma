//
//  ContentView.swift
//  EphemeralChroma
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

// Helper for iOS platform check
func isIPad() -> Bool {
    UIDevice.current.userInterfaceIdiom == .pad
}

func isIPhone() -> Bool {
    UIDevice.current.userInterfaceIdiom == .phone
}
#else
extension View {
    func dismissKeyboard() {
        // No-op for macOS
    }
}

// Helper for iOS platform check
func isIPad() -> Bool {
    false
}

func isIPhone() -> Bool {
    false
}
#endif

struct ContentView: View {
    
    @State var state: ChromaState = .init()
    @Environment(\.horizontalSizeClass) var horizontalSizeClass
    @State private var lastLogMessage: String?
    @State private var showingLogToast = false
    
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
                            if isIPad() {
                                logsView
                            } else {
                                logsView
                                    .frame(height: 400)
                            }
                        }
                    }
                }
            }
        }
        .onAppear {
            do {
                if !state.isInitialized {
                    try state.initialize()
                    state.refreshCollections()
                }
            } catch {
                // Just add to logs without showing alert
                state.addLog("Failed to initialize: \(error)")
            }
        }
        .onChange(of: state.logs) { oldLogs, newLogs in
            if isIPhone() {
                // Skip first log message
                if oldLogs.isEmpty && !newLogs.isEmpty {
                    return
                }
                if let lastLog = newLogs.last {
                    lastLogMessage = lastLog
                    showingLogToast = true
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                        showingLogToast = false
                    }
                }
            }
        }
        .overlay {
            if state.showingSuccess {
                SuccessToast()
                    .transition(.move(edge: .top).combined(with: .opacity))
            }
            
            if showingLogToast, let message = lastLogMessage {
                LogToast(message: message)
                    .transition(.move(edge: .top).combined(with: .opacity))
            }
        }
        .animation(.easeInOut, value: state.showingSuccess)
        .animation(.easeInOut, value: showingLogToast)
    }
    
    var headerView: some View {
        HeaderView(title: "Ephemeral Chroma Demo")
    }
    
    var databaseControls: some View {
        DatabaseControlsView(state: state)
    }
    
    var documentInputSection: some View {
        DocumentInputSectionView(state: state)
    }
    
    var querySection: some View {
        QuerySectionView(state: state)
    }
    
    var logsView: some View {
        LogsView(logs: $state.logs)
    }
}

// Helper to parse comma-separated floats
func parseEmbedding(_ text: String) -> [Float]? {
    let parts = text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
    let values = parts.compactMap { Float($0) }
    guard values.count == parts.count, !values.isEmpty else { return nil }
    return values
}
