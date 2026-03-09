document$.subscribe(() => {
  document
    .querySelectorAll('a[href="https://superset.ise.fiz-karlsruhe.de/superset/dashboard/mse-kg-dashboard/"]')
    .forEach(link => {
      link.target = "_blank";
      link.rel = "noopener noreferrer";
    });
});