# results_writer.py

class ResultsWriter:
    """
    Writes output lines to a file.
    """
    def __init__(self, filename):
        self.filename = filename
        # Optionally clear the file when initializing.
        with open(self.filename, "w") as file:
            file.write("")
    
    def write_line(self, line):
        with open(self.filename, "a") as file:
            file.write(line + "\n")
