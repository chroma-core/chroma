import { useState, useEffect } from "react";
import { remark } from "remark";
import remarkHtml from "remark-html";
import { remarkCustom } from "@/markdown";
import styles from "./markdown-content.module.css";

export default function MarkdownContent({ content, className }: { content: string, className?: string }) {
    const [rendering, setRendering] = useState(true);
    const [htmlBody, setHtmlBody] = useState(content);
    const [estimatedLines, setEstimatedLines] = useState(3);

    useEffect(() => {
        remark()
            .use(remarkHtml)
            .use(remarkCustom)
            .process(content)
            .then((result) => {
                setRendering(false);
                setHtmlBody(result.toString());
            });
        setEstimatedLines(content.length / 30);
    }, [content]);



    if (rendering) {
        return <MarkdownContentSkeleton lines={estimatedLines} className={className} />;
    }

    return <div
        className={`w-full ${styles.markdown} ${className}`}
        dangerouslySetInnerHTML={{ __html: htmlBody }}
    ></div>;
}

function MarkdownContentSkeleton({ lines, className }: { lines: number, className?: string }) {
    return <div className={`flex flex-col gap-1 w-full ${styles.markdown} ${className}`}>
        {Array.from({ length: lines }).map((_, i) => (
            <div key={i} className={`h-4 bg-gray-100 rounded-full animate-pulse mr-1`} />
        ))}
    </div>;
}
