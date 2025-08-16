import os
import sys
import webbrowser
import subprocess

def print_help():
    """Prints the available commands."""
    print("\nAvailable commands:")
    print("  open website <url>     - Opens the specified URL in a web browser.")
    print("  search <query>         - Searches for the query on Google.")
    print("  list files [path]      - Lists files in the current directory or a specified path.")
    print("  run <program>          - Runs a program (e.g., 'notepad', 'calc').")
    print("  help                   - Shows this help message.")
    print("  exit                   - Exits the program.")
    print("\nExample: open website https://www.google.com")

def main():
    """Main function to run the command loop."""
    print("Welcome to your personal command assistant.")
    print_help()

    while True:
        try:
            # Get user input in English. The input prompt is suppressed for non-interactive sessions.
            prompt = "\nEnter a command: " if sys.stdout.isatty() else ""
            user_input = input(prompt).strip()

            if not user_input:
                continue

            parts = user_input.split()
            command = parts[0].lower()
            args = parts[1:]

            if command == "exit":
                print("Goodbye!")
                break
            elif command == "help":
                print_help()
            elif command == "open" and args and args[0].lower() == "website":
                if len(args) > 1:
                    url = args[1]
                    if not url.startswith('http'):
                        url = 'https://' + url
                    print(f"Opening website: {url}")
                    # webbrowser.open(url) # This is disabled for the sandbox environment
                else:
                    print("Error: Please provide a URL.")
            elif command == "search":
                if args:
                    query = " ".join(args)
                    url = f"https://www.google.com/search?q={query}"
                    print(f"Searching for: {query}")
                    # webbrowser.open(url) # This is disabled for the sandbox environment
                else:
                    print("Error: Please provide a search query.")
            elif command == "list" and args and args[0].lower() == "files":
                path = args[1] if len(args) > 1 else "."
                try:
                    if os.path.isdir(path):
                        print(f"Listing files in: {os.path.abspath(path)}")
                        files = os.listdir(path)
                        for item in files:
                            print(item)
                    else:
                        print(f"Error: Directory not found at '{path}'")
                except Exception as e:
                    print(f"An error occurred: {e}")
            elif command == "run":
                if args:
                    program = args[0]
                    try:
                        print(f"Attempting to run '{program}'...")
                        # subprocess.Popen(program) # This is disabled for the sandbox environment
                        print(f"Successfully started '{program}'.")
                    except FileNotFoundError:
                        print(f"Error: Program '{program}' not found. Make sure it's in your system's PATH.")
                    except Exception as e:
                        print(f"An error occurred while trying to run '{program}': {e}")
                else:
                    print("Error: Please specify a program to run.")
            else:
                print(f"Unknown command: '{user_input}'")
                print("Type 'help' to see available commands.")

        except EOFError:
            # This handles piped input gracefully, allowing the script to exit after processing.
            print() # Print a newline for cleaner output
            break
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
