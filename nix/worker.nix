{
  buildPythonPackage,
  hatchling,
  valkey,
  boto3,
  pytesseract,
  tesseract,
  pdf2image,
  poppler-utils,
}:
buildPythonPackage {
  pname = "worker";
  version = "0.1.0";
  pyproject = true;
  src = ../worker;
  dependencies = [
    valkey
    boto3
    (
      (pytesseract.override {
        tesseract = tesseract.override {
          enableLanguages = [
            "eng"
            "osd"
          ];
        };
      }).overridePythonAttrs
        { doCheck = false; }
    )
    # HACK: there is a bug in nixpkgs and this is a workaround
    (pdf2image.overridePythonAttrs (old: {
      postPatch = (old.postPatch or "") + ''
        substituteInPlace pdf2image/pdf2image.py \
          --replace-fail 'poppler_path: str = None' \
                         'poppler_path: str = "${poppler-utils}/bin"'
      '';
    }))
  ];
  build-system = [ hatchling ];
  pythonImportsCheck = [ "worker.main" ];
  meta.mainProgram = "worker";
}
