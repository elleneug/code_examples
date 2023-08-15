# code for creating combined pdp plot with counts

def get_ft_weights(model, model_columns, mono_exists=False, monotone_dict = None):
    df_features = pd.DataFrame(model_columns, columns=['feature'])
    df_features['weight'] = model.feature_importances_
    if mono_exists:
        df_features = pd.merge(df_features, pd.DataFrame(list(monotone_dict.items()), columns = ['feature', 'monotonicity_trend']), on='feature')
    return df_features.sort_values(by='weight', ascending='False')

target = features_prefix + 'fraud_flag'
financial_filter_cond  = True
monotone_dict = {feature:1 for feature in model_columns}
importance_scores =    get_ft_weights(model,model_columns,mono_exists=True, monotone_dict = monotone_dict)
importance_scores.sort_values(by='weight',ascending=False,inplace=True)
df_num_stats_ind = pd.DataFrame()

for col in importance_scores.loc[importance_scores['weight'] > 0, 'feature']:
    print(col)
    pd_full_dataset_iter = X_train[model_columns].copy()
    pd_full_dataset_iter[target] = y_train.copy()
    calc_ind = pd_full_dataset_iter.groupby(col).agg({target: ['count', 'sum']}).reset_index()
    calc_ind.columns = ['col_value', 'count', 'predict_default']
    calc_ind = calc_ind.sort_values(by='count', ascending=False).reset_index(drop=True)
    calc_ind = calc_ind.iloc[:100]
    col_predicts = []


    for col_value in calc_ind['col_value']:
        pd_full_dataset_iter[col] = col_value
        pd_full_dataset_iter[target] = model.predict(pd_full_dataset_iter[model_columns])
        col_predicts.append(pd_full_dataset_iter[target].mean())

    pdp_df_iter = pd.DataFrame(list(calc_ind['col_value']), columns=['col_value'])
    pdp_df_iter['score'] = col_predicts
    pdp_df_iter['col'] = col
    pdp_df_iter = pdp_df_iter.sort_values(by='col_value').reset_index(drop=True)

    # Calculate the bin edges and bin names
    bin_edges = np.linspace(pdp_df_iter['col_value'].min(), pdp_df_iter['col_value'].max(), num=21)
    bin_names = [f'{round(bin_edges[i], 3)}-{round(bin_edges[i+1], 3)}' for i in range(len(bin_edges) - 1)]

    pdp_df_iter['bin'] = pd.cut(pdp_df_iter['col_value'], bins=bin_edges, labels=bin_names, include_lowest=True, duplicates = 'drop')

    pdp_df_iter = pd.merge(pdp_df_iter, calc_ind[['col_value', 'count']], on=['col_value'], how='inner')
    df_num_stats_ind = pd.concat([df_num_stats_ind, pdp_df_iter]).drop_duplicates()


    fig, ax1 = plt.subplots(figsize=(16, 8))
    ax2 = ax1.twinx()
    test_pdp = df_num_stats_ind.loc[df_num_stats_ind['col'] == col, ['col_value', 'score', 'count', 'bin']]
    test_pdp['col_value'] = test_pdp['col_value'].apply(lambda x: round(x, 2))
    test_pdp = test_pdp.groupby('bin').agg({'col_value':'mean','score': 'mean', 'count': 'sum'}).reset_index()
    test_pdp.columns = ['bin', 'col_value', 'score', 'count']
    test_pdp = test_pdp.sort_values(by='col_value')

    sns.barplot(x='bin', y='count', data=test_pdp, ax=ax1, color='white', edgecolor='red', linestyle='dashed', hatch='')
    sns.lineplot(x='bin', y='score', data=test_pdp, ax=ax2)

    ax1.set_xlim(ax2.get_xlim())  # Set x-axis limits of the line plot to match the bar plot



    ax2.set_ylabel('Count', color='red')
    ax1.set_xlabel('Column Value')
    ax1.set_ylabel('Score', color='blue')

    for tick in ax1.get_xticklabels():
        tick.set_rotation(90)
    #plt.xticks(rotation = 45)
    plt.title(f'PDP for {col}')
    plt.show()
