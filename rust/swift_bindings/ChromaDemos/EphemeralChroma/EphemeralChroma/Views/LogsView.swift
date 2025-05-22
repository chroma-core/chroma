//
//  LogsView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma


struct LogsView: View {
    
    @Binding var logs: [String]
    
    var body: some View {
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
