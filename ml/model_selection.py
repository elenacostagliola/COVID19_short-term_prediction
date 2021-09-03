from multiprocessing import cpu_count

from joblib import Parallel, delayed

from .performance_measures import rmse
from .sarimax import SarimaxException


def get_sarima_configurations(p_values, d_values, q_values,
                              P_values, D_values, Q_values,
                              m_values, t_values):
    configurations = []

    for p in p_values:
        for d in d_values:
            for q in q_values:
                for P in P_values:
                    for D in D_values:
                        for Q in Q_values:
                            for m in m_values:
                                for t in t_values:
                                    cfg = {"arima_order": (p, d, q),
                                           "seasonal_order": (P, D, Q, m),
                                           "trend": t}
                                    configurations.append(cfg)
    return configurations


def get_performance(data, model, cfg, performance_measure, transformation=None, splits=None, **kwargs):
    m = model(data=data, config=cfg, transformation=transformation, **kwargs)
    if isinstance(performance_measure, str):
        if performance_measure.lower() == "aic":
            m.fit()
            return m.aic, None
    else:
        assert splits is not None, "Missing splits"
        m.cross_validate(splits=splits, performance_measure=performance_measure)
        return m.test_performance[performance_measure.__name__.upper()], m.test_performance[
            performance_measure.__name__.upper() + "_se"]


def score_model(data, model, cfg, iteration_count, performance_measure, transformation=None, splits=None, debug=False, **kwargs):
    if (iteration_count + 1) % 100 == 0:
        print(f"Scoring configuration {iteration_count + 1}")

    try:
        result = get_performance(data, model, cfg, performance_measure, transformation=transformation, splits=splits,
                                 convergence_warnings=debug, **kwargs)
    except SarimaxException:
        result = None

    return {"cfg": cfg, "score_mean": result[0] if result is not None else None,
            "score_se": result[1] if result is not None else None}


def grid_search(data=None, model=None, configurations=None,
                performance_measure=rmse, splits=None,
                parallel=False, n_jobs=cpu_count(), parallel_backend="loky",
                debug=False, transformation=None,
                **kwargs):
    assert data is not None, "Missing data"
    assert model is not None, "Missing model"
    assert configurations is not None, "Missing list of configurations"

    print(f"Scoring {len(configurations)} configurations{' in parallel' if parallel else ''}...")

    results = None

    if parallel:
        # execute configs in parallel
        executor = Parallel(n_jobs=n_jobs, backend=parallel_backend)
        tasks = (delayed(score_model)(data, model, cfg, i, performance_measure, transformation=transformation,
                                      splits=splits, debug=debug, **kwargs) for i, cfg in enumerate(configurations))
        results = executor(tasks)
    else:
        results = [score_model(data, model, cfg, i, performance_measure, transformation=transformation,
                               splits=splits, debug=debug, **kwargs) for i, cfg in enumerate(configurations)]

    results = [r for r in results if r["score_mean"] is not None]

    # For some performance measures, pick max
    sort_descending = False
    if isinstance(performance_measure, str):
        if performance_measure.lower() == "aic":
            sort_descending = True

    results.sort(key=lambda x: x["score_mean"], reverse=sort_descending)
    return results
