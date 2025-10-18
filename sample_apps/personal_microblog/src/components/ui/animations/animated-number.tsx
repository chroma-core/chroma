import { useEffect, useState } from "react";
import { animate, useMotionValue, useTransform } from "framer-motion";

export default function AnimatedNumber({ number }: { number: number }) {
    const count = useMotionValue(0);
    const rounded = useTransform(count, (latest) => Math.round(latest));
    const [displayValue, setDisplayValue] = useState(0);

    useEffect(() => {
        const animation = animate(count, number, {
            duration: .2 * number,
            ease: "easeOut",
        });

        return animation.stop;
    }, [count, number]);

    useEffect(() => {
        const unsubscribe = rounded.on("change", (latest) => {
            setDisplayValue(latest);
        });

        return unsubscribe;
    }, [rounded]);

    return <span>{displayValue}</span>;
}
