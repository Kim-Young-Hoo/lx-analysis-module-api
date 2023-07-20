from typing import List

import pandas as pd
from fastapi import Depends
from sqlalchemy import or_, inspect, text
from sqlalchemy.orm import Session

from db.session import get_db
from schemas.analysis import CreateCorrelation, CreateRegression, ShowAnalysis, CreateClustering, AnalysisResult
from analysis_module.regression_module import RegressionModule
from analysis_module.correlation_module import CorrelationModule
from analysis_module.clustering_module import GMMModule
from db.models.data import GgsStatis


def create_correlation_analysis(analysis_data: CreateCorrelation, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(analysis_data.variable_list,
                                                    analysis_data.year,
                                                    analysis_data.value_period_type,
                                                    db)

    correlation_module = CorrelationModule(pivoted_df.iloc[:, 3:], dat_no_dat_nm_dict)
    corr_result = ShowAnalysis(data=[])

    pair_plot = correlation_module.save_pair_plot(),
    heatmap_plot = correlation_module.save_heatmap_plot(),
    descriptive_statistics_table = correlation_module.save_descriptive_statistics_table()

    corr_result.data.append(AnalysisResult(title="산점도행렬", result=pair_plot[0], format="base64"))
    corr_result.data.append(AnalysisResult(title="상관계수 히트맵", result=heatmap_plot[0], format="base64"))
    corr_result.data.append(AnalysisResult(title="기술통계", result=descriptive_statistics_table, format="base64"))
    return corr_result


def create_regression_analysis(analysis_data: CreateRegression, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(
        analysis_data.independent_variable_list + [analysis_data.dependent_variable],
        analysis_data.year,
        analysis_data.value_period_type,
        db)

    regression_module = RegressionModule(pivoted_df, analysis_data.dependent_variable, dat_no_dat_nm_dict)
    regression_module.fit()
    regression_summary_table = regression_module.get_result_summary()
    anova_table = regression_module.get_anova_lm()
    descriptive_statistics_table = regression_module.save_descriptive_statistics_table()

    regression_result = ShowAnalysis(data=[])
    regression_result.data.append(AnalysisResult(title="모형요약표", result=regression_summary_table, format="html"))
    regression_result.data.append(AnalysisResult(title="분산분석표", result=anova_table, format="base64"))
    regression_result.data.append(AnalysisResult(title="기술통계", result=descriptive_statistics_table, format="base64"))
    return regression_result


def create_clustering_analysis(analysis_data: CreateClustering, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(analysis_data.variable_list,
                                                    analysis_data.year,
                                                    analysis_data.value_period_type,
                                                    db)


def get_value_period_list(value_period_type: str) -> List[str]:
    if value_period_type == "year":
        return ["yr_vl"]
    elif value_period_type == "month":
        return ["jan", "feb", "mar", "apr", "may", "jun", "july", "aug", "sep", "oct", "nov", "dec"]
    elif value_period_type == "quarter":
        return ["qu_1", "qu_2", "qu_3", "qu_4"]
    elif value_period_type == "half":
        return ["ht_1", "ht_2"]


def get_pivoted_df(variable_list: List[str], year: str, value_period_type: str, db: Session):
    value_period_list = get_value_period_list(value_period_type)

    query_template = """
        SELECT
            stat.stdg_cd,
            stat.yr,
            stat.dat_no,
            info.dat_nm,
            stat.jan,
            stat.feb,
            stat.mar,
            stat.apr,
            stat.may,
            stat.jun,
            stat.july,
            stat.aug,
            stat.sep,
            stat.oct,
            stat.nov,
            stat.dec,
            stat.qu_1,
            stat.qu_2,
            stat.qu_3,
            stat.qu_4,
            stat.ht_1,
            stat.ht_2,
            stat.yr_vl
        FROM ggs_statis stat
        JOIN ggs_data_info info ON stat.dat_no = info.dat_no
        WHERE stat.dat_no IN ({})
        AND yr='{}'
    """
    placeholders = ', '.join([':param{}'.format(i) for i in range(len(variable_list))])
    query = text(query_template.format(placeholders, year))
    params = {f'param{i}': value for i, value in enumerate(variable_list)}
    result = db.execute(query, params)

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    melted_df = pd.melt(df, id_vars=['yr', 'stdg_cd', 'dat_no', 'dat_nm'], value_vars=value_period_list)
    pivoted_df = pd.pivot_table(melted_df, values='value', index=['yr', 'stdg_cd', 'variable'], columns='dat_no')

    dat_no_dat_nm_dict = df.set_index('dat_no')['dat_nm'].to_dict()

    # pivoted_df.to_csv("analysis_module/dataset/data.csv")
    # pivoted_df = pd.read_csv("analysis_module/dataset/data.csv")
    pivoted_df.to_csv("./data.csv")
    pivoted_df = pd.read_csv("./data.csv")
    return pivoted_df, dat_no_dat_nm_dict


if __name__ == '__main__':
    # create_correlation = CreateCorrelation(
    #     variable_list=["M0002001" + str(i) for i in range(0, 10)],
    #     year="2021",
    #     value_period_type="year",
    #     testing_side="both",
    #     valid_pvalue_accent=True
    # )

    # df, dic = get_pivoted_df(create_correlation.variable_list, create_correlation.year,
    #                          create_correlation.value_period_type, get_db().__next__())
    # correlation_module = CorrelationModule(df.iloc[:, 3:], dic)
    # pair_plot = correlation_module.save_pair_plot(),
    # heatmap_plot = correlation_module.save_heatmap_plot(),
    # descriptive_statistics_table = correlation_module.save_descriptive_statistics_table()

    create_regression = CreateRegression(
        independent_variable_list=["M0002001" + str(i) for i in range(0, 10)],
        dependent_variable="M00020011",
        year="2021",
        value_period_type="year",
        testing_side="both",
        valid_pvalue_accent=True
    )

    df, dic = get_pivoted_df(create_regression.independent_variable_list + [create_regression.dependent_variable],
                             create_regression.year,
                             create_regression.value_period_type,
                             get_db().__next__())

    regression_module = RegressionModule(df.iloc[:, 3:], create_regression.dependent_variable, dic)
    regression_module.fit()
    print(regression_module.get_anova_lm())
    # regression_module.get_result_summary()

