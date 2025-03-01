import subprocess


def test_cli_wrapper():
    result = subprocess.run(
        ["python", "-m", "chromadb.cli.cli", "--help"],
        capture_output=True,
        text=True,
        check=True
    )
    help_output = result.stdout
    print("Help output:", help_output)

    assert "run" in help_output, "'run' not found in help output"
