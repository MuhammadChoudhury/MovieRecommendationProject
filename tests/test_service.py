# tests/test_service.py
import pytest
from fastapi.testclient import TestClient
from service.app import app  
from unittest.mock import MagicMock


@pytest.fixture
def client():

    
    app.state.models = {
        "popularity": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "item_cf": {"movie_ids": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]}
    }
    app.state.model_version = "v_test_123"
    app.state.kafka_producer = MagicMock()

    with TestClient(app) as test_client:
        yield test_client
    

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
    # Check that it returns the first 5 items from our fake model
    assert data["movie_ids"] == [1, 2, 3, 4, 5]

def test_recommend_item_cf(client):
    # User 124 is even, should get the 'item_cf' model
    response = client.get("/recommend/124?k=3")
    data = response.json()
    assert response.status_code == 200
    assert data["user_id"] == 124
    assert data["model_used"] == "item_cf"
    # Check that it returns the first 3 items from our fake model
    assert data["movie_ids"] == [10, 9, 8]

def test_healthz_fails_if_models_not_loaded(client):
    # Test the safety check
    app.state.models = None # Simulate a failed model load
    
    response = client.get("/healthz")
    assert response.status_code == 503
    assert "not loaded" in response.json()["detail"]