//
//  HeaderView.swift
//  EphemeralChroma
//
//  Created by Nicholas Arner on 5/21/25.
//

import SwiftUI
import Chroma

func logInView(_ s: String) -> EmptyView {
    print(s)
    return EmptyView()
}

struct HeaderView: View {
    
    let title: String
    
    var body: some View {
        VStack(spacing: 8) {
            Text(title)
                .font(.title)
                .bold()
        }
        .frame(maxWidth: .infinity)
        .padding()
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
