"use client";

import { useId } from "react";
import styles from "./logo.module.css";

/**
 * This is the X logo that transforms into the Chroma logo when hovered.
 */
export default function Logo({ size }: { size: number }) {
    const logoId = useId();
    const middleXState = "M 13 22 v -7 Q 22 2 22 2 v 7 Q 13 22 13 22 Z";
    const middleChromaState = "M 13 22 v -10 Q 13 2 22 2 v 10 Q 22 22 13 22 Z";
    return (
        <div id={`${logoId}`} onMouseOver={(e) => e.stopPropagation()} onMouseOut={(e) => e.stopPropagation()}>
            <svg width={`${size * 1.5}px`} height={`${size}px`} viewBox="0 0 36 24" fill="none" xmlns="http://www.w3.org/2000/svg" className={styles.logo}>
                <circle cx="13" cy="12" r="10" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" className={styles.left} />
                <circle cx="23" cy="12" r="10" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" className={styles.right} />
                <path d={middleXState} fill="white" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" className={styles.center}>
                    <animate
                        attributeName="d"
                        from={middleXState}
                        to={middleChromaState}
                        begin={`${logoId}.mouseover`}
                        fill="freeze"
                        restart="whenNotActive"
                        dur=".4s"
                    />
                    <animate
                        attributeName="d"
                        from={middleChromaState}
                        to={middleXState}
                        begin={`${logoId}.mouseout`}
                        fill="freeze"
                        restart="whenNotActive"
                        dur=".4s"
                    />
                </path>
                <rect width="100%" height="100%" />
            </svg>
        </div>
    );
}
