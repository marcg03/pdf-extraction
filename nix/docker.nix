{ dockerTools, app }:
dockerTools.buildImage {
  name = app.pname;
  tag = "latest";

  copyToRoot = [ app ];

  config.Cmd = [ "/bin/${app.meta.mainProgram}" ];
}
