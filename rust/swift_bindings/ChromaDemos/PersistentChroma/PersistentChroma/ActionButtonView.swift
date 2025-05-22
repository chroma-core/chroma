//
//  ActionButtonView.swift
//  PersistentChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

// Simple action button with manual styling
struct ActionButton: View, Equatable {
    
    // i.e. ignore `action` closure
    static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.title == rhs.title
        && lhs.disabled == rhs.disabled
    }
    
    let title: String
    var disabled: Bool = false
    
    var action: () throws -> Void
    
    @State private var inProgress = false
    @State private var errorMessage: String? = nil
    
    var body: some View {
        Button {
            dismissKeyboard()
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
            Text(title)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
            
            // .overlay, so appearance/disappearance of progress-view does not affect layout
                .overlay(alignment: .trailing) {
                    if inProgress {
                        ProgressView()
                            .controlSize(.small)
                            .frame(width: 16, height: 16)
                    }
                }
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
