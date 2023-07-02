with (import <nixpkgs> {});
with python311Packages;
stdenv.mkDerivation {
  name = "pip-env";
  buildInputs = [
    # System requirements.
    readline

    # to run translator
    nodejs
    yarn

    # Python requirements (enough to get a virtualenv going).
    python311Full
    virtualenv
    setuptools
    pip
  ];
  src = null;
  shellHook = ''
    # Allow the use of wheels.
    SOURCE_DATE_EPOCH=$(date +%s)

    if [ ! -d "venv" ]; then
        virtualenv venv
    fi

    source venv/bin/activate
  '';
}
