# tests/test_service.py
import pytest
from fastapi.testclient import TestClient
from service.app import app
from unittest.mock import MagicMock

# This "fixture" is a setup function that runs before each test
# It uses 'mocker' from the pytest-mock library
@pytest.fixture
def client(mocker):
    """
    Mocks all external dependencies (Kafka, S3) *before* creating the TestClient.
    This prevents the app's 'lifespan' function from making real network calls.
    """
    
    # 1. Mock the Kafka Producer
    # Find 'Producer' in 'service.app' and replace it with a fake
    mock_producer = MagicMock()
    mocker.patch('service.app.Producer', return_value=mock_producer)

    # 2. Mock the S3FileSystem
    # Find 's3fs.S3FileSystem' in 'service.app' and replace it
    mock_s3 = MagicMock()
    mocker.patch('service.app.s3fs.S3FileSystem', return_value=mock_s3)
    
    # 3. Mock the S3 file 'open' call for 'latest.txt'
    # We create a fake file that, when 'read().strip()' is called, returns a test version
    mock_file = MagicMock()
    mock_file.__enter__.return_value.read.return_value.strip.return_value = "v_test_123"
    mock_s3.open.return_value = mock_file

    # 4. Mock 'joblib.load'
    # We tell it to return fake models when it's called
    mock_models = {
        "popularity": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], # Fake popularity model
        "item_cf": {"movie_ids": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]} # Fake Item-CF model
    }
    # This mock will return the pop model on the first call, and the cf model on the second
    mocker.patch('service.app.joblib.load', side_effect=[
        mock_models["popularity"],
        mock_models["item_cf"]
    ])
    
    # 5. Now, create the TestClient.
    # The app's 'lifespan' function will run, but it will use all our mocks.
    with TestClient(app) as test_client:
        yield test_client

# --- Your Updated Tests ---
# They now use the 'client' fixture, which provides the mocked app

def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "v_test_123"}

def test_recommend_popularity(client):
    # User 123 is odd, should get the 'popularity' model
    response = client.get("/recommend/123?k=5")
    data = response.json()
    assert response.status_code == 200
    assert data["user_id"] == 123
    assert data["model_used"] == "popularity"
    assert data["movie_ids"] == [1, 2, 3, 4, 5]

def test_recommend_item_cf(client):
    # User 124 is even, should get the 'item_cf' model
    response = client.get("/recommend/124?k=3")
    data = response.json()
    assert response.status_code == 200
    assert data["user_id"] == 124
    assert data["model_used"] == "item_cf"
    assert data["movie_ids"] == [10, 9, 8]

def test_healthz_fails_if_models_not_loaded(mocker):
    """
    A separate test to ensure the API fails gracefully
    if the models *actually* fail to load.
    """
    # Mock all dependencies, but make S3 'open' raise an error
    mocker.patch('service.app.Producer', return_value=MagicMock())
    mock_s3 = MagicMock()
    mock_s3.open.side_effect = FileNotFoundError("Mock S3 Error") # Simulate S3 failing
    mocker.patch('service.app.s3fs.S3FileSystem', return_value=mock_s3)
    
    # Create a client *inside* this test to use these specific mocks
    with TestClient(app) as local_client:
        # Check that healthz fails
        response = local_client.get("/healthz")
        assert response.status_code == 503
        assert "not loaded" in response.json()["detail"]
        
        # Check that /recommend also fails
        response = local_client.get("/recommend/123")
        assert response.status_code == 503