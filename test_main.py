from main import main_results


def test_function():
    return main_results()


if __name__ == "__main__":
    result = test_function()
    assert result["extract_to"] == "success", "Extract failed"
    assert result["transform_db"] == "success", "Transform and load failed"
    assert result["join"] == "Join Success", "Join query failed"
    assert result["aggregate"] == "Aggregate Success", "Aggregate query failed"
    assert result["sort"] == "Sort Success", "Sort query failed"
    print("All tests passed.")
