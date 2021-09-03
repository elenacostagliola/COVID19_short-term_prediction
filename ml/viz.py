import matplotlib.pyplot as plt


def plot_model(fit_result_df, quantity=None, title=None, residuals=None, confint="95%"):
    if residuals is not None:
        fig, axes = plt.subplots(2, 1,
                                 figsize=(8, 5),
                                 gridspec_kw={'height_ratios': [3, 1]},
                                 sharex=True)
    else:
        fig, axes = plt.subplots(1, 1,
                                 figsize=(8, 3))
        axes = [axes]

    # Plot data
    fit_result_df["data"].plot(ax=axes[0], lw=1, color="k", alpha=0.5)

    # Plot fitted values
    fit_result_df["fitted"].plot(ax=axes[0], lw=2, color="#3528fd", alpha=.7)

    # Plot prediction
    fit_result_df['forecast'].plot(ax=axes[0], color="r", lw=2, alpha=.7)
    axes[0].fill_between(fit_result_df.index, fit_result_df['lower_ci'], fit_result_df['upper_ci'],
                         color='k', alpha=0.1, label=f"{confint} C.I.")

    # Plot residuals
    if residuals is not None:
        residuals.plot(ax=axes[1], color="#3528fd", lw=2, alpha=0.7)
        axes[1].plot([fit_result_df.index[0], fit_result_df.index[-1]], [0, 0], color="k", alpha=0.4, linestyle="--")

    # Axis labels
    if residuals is not None:
        axes[1].set_xlabel("Time")
        axes[1].set_ylabel("Residuals")
    else:
        axes[0].set_xlabel("Time")

    axes[0].set_ylabel(quantity if quantity is not None else "y")

    # Limits
    axes[0].set_xlim([fit_result_df.index[0], fit_result_df.index[-1]])

    # Legend
    axes[0].legend()

    # Title
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Add grids
    for ax in axes:
        ax.grid(which="major", alpha=0.5)
        ax.grid(which="minor", alpha=0.2)
    plt.tight_layout()

    return fig, axes
