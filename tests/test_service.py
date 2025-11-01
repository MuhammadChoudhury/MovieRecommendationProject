from fastapi.testclient import TestClient
from service.app import app 

client = TestClient(app)

def test_healthz():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "1.0"}

def test_recommend_popularity():
    response = client.get("/recommend/123?model=popularity&k=5")
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == 123
    assert len(data["movie_ids"]) == 5
    assert data["model"] == "popularity"

def test_invalid_model():
    response = client.get("/recommend/123?model=invalid_model")
    assert response.status_code == 400
    assert "not found" in response.json()["detail"]