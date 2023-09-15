from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli
from sdrf_pipelines.zooma.zooma import SlimOlsClient
from sdrf_pipelines.zooma.zooma import Zooma

TESTDATA_PATH = (Path(__file__).parent.parent / "testdata").resolve()


@pytest.fixture(autouse=True)
def change_test_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def test_validate_srdf(change_test_dir):
    """
    :return:
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["validate-sdrf", "--sdrf_file", f"{TESTDATA_PATH}/PXD000288.sdrf.tsv", "--check_ms"])
    assert result.exit_code == 0, result.output
    assert "ERROR" not in result.output.upper()
    print("validate sdrf " + result.output)
    """
    # Currently fails with this error
    E       AssertionError: The following columns are mandatory and not present in the SDRF: comment[technical replicate] -- ERROR
    E         The column comment[technical replicate] is not present in the SDRF -- ERROR
    E         The following columns are mandatory and not present in the SDRF: comment[technical replicate] -- ERROR
    E         The column comment[technical replicate] is not present in the SDRF -- ERROR
    E         There were validation errors.
    E         
    E       assert True == 0
    E        +  where True = <Result SystemExit(True)>.exit_code
    """


def test_convert_openms(change_test_dir):
    """
    :return:
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["convert-openms", "-t2", "-s", f"{TESTDATA_PATH}/PXD000288.sdrf.tsv"])
    assert result.exit_code == 0
    assert Path("experimental_design.tsv").exists()
    assert Path("openms.tsv").exists()

    assert len(pd.read_csv("experimental_design.tsv", sep="\t")) > 2
    assert len(pd.read_csv("openms.tsv", sep="\t")) > 2

    print("convert to openms " + result.output)
    assert "ERROR" not in result.output.upper()


def test_convert_openms_file_extensions(change_test_dir):
    """
    :return:
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["convert-openms", "-t2", "-s", f"{TESTDATA_PATH}/PXD000288.sdrf.tsv", "--extension_convert", "raw:mzML"],
    )
    assert result.exit_code == 0, result.output
    assert "ERROR" not in result.output.upper()
    filepaths = list(pd.read_csv("experimental_design.tsv", sep="\t")["Spectra_Filepath"])
    filepaths = [x for x in filepaths if x.startswith("2012")]
    assert all([x.endswith(".mzML") for x in filepaths])


def test_convert_openms_file_extensions_dotd(change_test_dir):
    """
    :return:
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "convert-openms",
            "-t2",
            "-s",
            f"{TESTDATA_PATH}/quantms_dia_dotd_sample.sdrf.tsv",
            "--extension_convert",
            ".zip:",
        ],
    )
    assert result.exit_code == 0
    assert len(pd.read_csv("experimental_design.tsv", sep="\t")) > 2, result.output
    assert len(pd.read_csv("openms.tsv", sep="\t")) > 2, result.output
    filepaths = list(pd.read_csv("experimental_design.tsv", sep="\t")["Spectra_Filepath"])
    filepaths = [x for x in filepaths if x.startswith("3817")]
    assert all([x.endswith(".d") for x in filepaths])
    assert "ERROR" not in result.output.upper()


def test_bioontologies():
    keyword = "human"
    client = Zooma()
    results = client.recommender(keyword, filters="ontologies:[nbcitaxon]")
    ols_terms = client.process_zooma_results(results)
    print(ols_terms)

    ols_client = SlimOlsClient()
    for ols_term in ols_terms:
        terms = ols_client.get_term_from_url(ols_term["ols_url"], ontology="ncbitaxon")
        print(*terms, sep="\n")

    keyword = "Lung adenocarcinoma"
    client = Zooma()
    results = client.recommender(keyword)
    ols_terms = client.process_zooma_results(results)
    print(ols_terms)


if __name__ == "__main__":
    test_bioontologies()
    test_validate_srdf()
    test_convert_openms()
