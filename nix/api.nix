{
  buildPythonPackage,
  fastapi,
  uvicorn,
  hatchling,
  python-multipart,
  valkey,
  aioboto3,
}:
buildPythonPackage {
  pname = "api";
  version = "0.1.0";
  pyproject = true;
  src = ../api;
  dependencies = [
    fastapi
    uvicorn
    valkey
    python-multipart
    aioboto3
  ];
  build-system = [ hatchling ];
  pythonImportsCheck = [ "api.main" ];
  meta.mainProgram = "api";
}
