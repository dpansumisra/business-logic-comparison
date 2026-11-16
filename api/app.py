"""FastAPI application entry point for Universal Data Format & Semantics Knowledge Repository."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.knowledge import router as knowledge_router
from api.routes.search import router as search_router
from api.routes.mappings import router as mappings_router
from api.routes.semantics import router as semantics_router
from api.routes.sources import router as sources_router
from api.routes.admin import router as admin_router
from api.routes.compare import router as compare_router


def create_app() -> FastAPI:
    """Builds and configures the FastAPI knowledge service application."""
    app = FastAPI(
        title="Universal Data Format & Semantics Knowledge Repository",
        version="1.0.0",
        description="Machine-readable knowledge repository for cross-language migration engines and semantic RCA.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(knowledge_router)
    app.include_router(search_router)
    app.include_router(mappings_router)
    app.include_router(semantics_router)
    app.include_router(sources_router)
    app.include_router(admin_router)
    app.include_router(compare_router)

    @app.get("/")
    def root():
        return {
            "name": "Universal Data Format & Semantics Knowledge Repository",
            "version": "1.0.0",
            "status": "online",
            "compare_ui": "/compare",
            "docs_url": "/docs"
        }

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
