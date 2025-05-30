import { useState, useEffect } from "react";
import { remark } from "remark";
import remarkHtml from "remark-html";
import { remarkCustom } from "@/markdown";
import styles from "./markdown-content.module.css";

export default function MarkdownContent({ content, className }: { content: string, className?: string }) {
    const [rendering, setRendering] = useState(true);
    const [htmlBody, setHtmlBody] = useState(content);

    useEffect(() => {
        remark()
            .use(remarkHtml)
            .use(remarkCustom)
            .process(content)
            .then((result) => {
                setRendering(false);
                setHtmlBody(result.toString());
            });
    }, [content]);

    if (rendering) {
        return <MarkdownContentSkeleton lines={3} className={className} />;
    }

    return <div
        className={`w-full ${styles.markdown} ${className}`}
        dangerouslySetInnerHTML={{ __html: htmlBody }}
    ></div>;
}

function MarkdownContentSkeleton({lines, className}: {lines: number, className?: string}) {
    return <div className={`w-full ${styles.markdown} ${className}`}>
        {Array.from({length: lines}).map((_, i) => (
            <div key={i} className={`h-4 bg-gray-300 rounded-full animate-pulse mr-1`} />
        ))}
    </div>;
}
