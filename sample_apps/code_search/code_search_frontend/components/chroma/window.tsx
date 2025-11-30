export default function Window(props: {children: React.ReactNode}) {
    return (
        <div className="bg-neutral-900 rounded-lg shadow-lg p-4">
            {props.children}
        </div>
    );}