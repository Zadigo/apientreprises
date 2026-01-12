from apientreprises.tasks import create_file

def test_create_file():
    data = {"key": "value"}
    create_file.apply(args=(data,))
    # Further assertions can be added here to verify file creation
