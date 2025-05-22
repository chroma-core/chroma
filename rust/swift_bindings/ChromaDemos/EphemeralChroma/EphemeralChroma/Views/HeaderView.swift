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
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
        .padding()
        .background(Color.clear)
        .frame(maxWidth: .infinity, alignment: .center)
    }
}

