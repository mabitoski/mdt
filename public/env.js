(() => {
  const host = (window.location.hostname || "").toLowerCase();
  const isDev = host.includes("dev") || window.location.port === "3001";

  if (isDev) {
    document.documentElement.classList.add("env-dev");
    document.documentElement.setAttribute("data-env", "dev");
  }
})();
