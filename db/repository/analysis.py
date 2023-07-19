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
    pivoted_df = get_pivoted_df(analysis_data.variable_list, db)
    correlation_module = CorrelationModule(pivoted_df.iloc[:, 2:])
    corr_result = ShowAnalysis(data=[])

    pair_plot = correlation_module.save_pair_plot(),
    heatmap_plot = correlation_module.save_heatmap_plot(),
    descriptive_statistics_table = correlation_module.save_descriptive_statistics_table()

    corr_result.data.append(AnalysisResult(title="산점도행렬", result=pair_plot[0], format="base64"))
    corr_result.data.append(AnalysisResult(title="상관계수 히트맵", result=heatmap_plot[0], format="base64"))
    corr_result.data.append(AnalysisResult(title="기술통계", result=descriptive_statistics_table, format="base64"))
    return corr_result


def create_regression_analysis(analysis_data: CreateRegression, db: Session):
    pivoted_df = get_pivoted_df(analysis_data.independent_variable_list + [analysis_data.dependent_variable], db)
    print(pivoted_df)
    regression_module = RegressionModule(pivoted_df, analysis_data.dependent_variable)
    regression_module.fit()
    regression_result = ShowRegression(
        regression_summary_table=regression_module.get_result_summary(),
        anova_table=regression_module.get_anova_lm(),
        descriptive_statistics_table=regression_module.save_descriptive_statistics_table()
    )
    return regression_result


def create_clustering_analysis(analysis_data: CreateClustering, db: Session):
    pivoted_df = get_pivoted_df(analysis_data.variable_list, db)


def get_pivoted_df(variable_list: List[str], db: Session):
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
    """
    # Generate the placeholders for the parameter values
    placeholders = ', '.join([':param{}'.format(i) for i in range(len(variable_list))])

    # Construct the query by inserting the placeholders
    query = text(query_template.format(placeholders))

    # Bind the parameter values
    params = {f'param{i}': value for i, value in enumerate(variable_list)}

    # Execute the query with the parameters
    result = db.execute(query, params)

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    pivoted_df = pd.pivot_table(df, values='yr_vl', index=['yr', 'stdg_cd'], columns='dat_no')
    pivoted_df.to_csv("analysis_module/dataset/data.csv")
    pivoted_df = pd.read_csv("analysis_module/dataset/data.csv")
    # pivoted_df.to_csv("./data.csv")
    # pivoted_df = pd.read_csv("./data.csv")
    return pivoted_df


if __name__ == '__main__':
    create_correlation = CreateCorrelation(
        variable_list=["M00026007", "M00026008", "M00026010", "M00026009"],
        year="2021",
        value_period_type="yr_vl",
        testing_side="both",
        valid_pvalue_accent=True
    )

    df = get_pivoted_df(create_correlation.variable_list, get_db().__next__())
    correlation_module = CorrelationModule(df.iloc[:, 2:])
    pair_plot = correlation_module.save_pair_plot(),
    heatmap_plot = correlation_module.save_heatmap_plot(),
    descriptive_statistics_table = correlation_module.save_descriptive_statistics_table()
