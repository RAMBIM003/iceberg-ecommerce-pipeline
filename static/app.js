// FETCH METRICS
fetch("/metrics")
  .then(res => res.json())
  .then(data => {
    document.getElementById("orders").innerText = data.total_orders;
    document.getElementById("revenue").innerText = data.total_revenue;
    document.getElementById("avg").innerText = data.avg_order_value;
  });

// FETCH STATUS + DRAW CHART
fetch("/status")
  .then(res => res.json())
  .then(data => {
    const labels = data.map(d => d.status);
    const values = data.map(d => d.count);

    new Chart(document.getElementById("statusChart"), {
      type: "bar",
      data: {
        labels: labels,
        datasets: [{
          label: "Orders by Status",
          data: values
        }]
      }
    });
  });