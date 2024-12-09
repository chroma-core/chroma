import React from "react";
import katex from 'katex';
import 'katex/dist/katex.min.css';

const Latex:React.FC<{children: React.ReactNode}> = ({children}) => {
    const content = React.Children.toArray(children).join('');
    try {
        return (
            <span
                dangerouslySetInnerHTML={{
                    __html: katex.renderToString(content, {
                        throwOnError: false,
                    }),
                }}
            />
        );
    } catch (error) {
        console.error(error);
        return <span style={{ color: 'red' }}>Error rendering LaTeX</span>;
    }
}

export default Latex;