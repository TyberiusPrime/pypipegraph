if __name__ == "__main__":
    modules_to_test = ["test_pypipegraph", "test_plotjobs"]

    import os

    os.environ["PYPIPEGRAPH_DO_COVERAGE"] = os.path.abspath(
        os.path.join(os.path.dirname(__file__), ".coveragerc")
    )
    import coverage

    with open(".coveragerc", "wb") as op:
        op.write(
            b"""
[run]
data_file = %s
parallel=True

[report]
include = *pypipegraph*
        """
            % (
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), ".coverageX"
                ).encode("utf-8")
            )
        )

    cov = coverage.coverage(
        source=[os.path.abspath("../")], config_file=".coveragerc", data_suffix=True
    )
    cov.start()

    import nose
    import sys

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

    try:
        nose.core.runmodule(
            modules_to_test,
            argv=sys.argv + ["--with-progressive", "--nologcapture"],
            exit=False,
        )  # log capture get's a ton of output from the pipegraph. enable if you need it
    except Exception as e:
        print("error")
        print(e)
        pass
    cov.stop()
    cov.save()
    # os.system('coverage combine')
    # os.system('coverage html -d covhtml')
    print("coverage report is in covhtml/index.html")
