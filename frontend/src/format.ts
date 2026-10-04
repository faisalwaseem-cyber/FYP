export function formatPkr(value: string | number): string {
  const raw = String(value).trim();
  if (!/^-?\d+(\.\d+)?$/.test(raw)) return "PKR --";

  const negative = raw.startsWith("-");
  const unsigned = negative ? raw.slice(1) : raw;
  const [whole, fraction = ""] = unsigned.split(".");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  const amount = fraction ? `${grouped}.${fraction}` : grouped;
  return `${negative ? "-" : ""}PKR ${amount}`;
}