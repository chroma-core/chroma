export default function Box(props: { title: string, children: React.ReactNode, className?: string }) {
    return (
        <div className={"mac-style mac-style-hover row-span-3 flex grow flex-col justify-between border border-black " + props.className}>
            <div className="px-5 pt-3 pb-2 font-mono text-md uppercase">{props.title}</div>
            <hr className="mt-1 mb-3"></hr>
            <div className="mb-4 flex flex-col gap-4 px-5 text-sm">
                {props.children}
            </div>
        </div>
    );
}