from rich.console import Console
from rich.table import Table
import logging

console = Console()

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

def log_node_start(node_id, logic, params):
    console.rule(f"[bold blue]Starting Node {node_id} ({logic})[/bold blue]")
    console.print(f"[cyan]Params:[/cyan] {params}")

def log_node_output(node_id, output):
    console.print(f"[green]Output from Node {node_id}:[/green] {output}")
    console.rule()

def log_pipeline_start(trigger_id, flow_id):
    console.rule(f"[bold magenta]Pipeline Triggered[/bold magenta]")
    console.print(f"[yellow]Trigger ID:[/yellow] {trigger_id}")
    console.print(f"[yellow]Flow ID:[/yellow] {flow_id}")

def log_pipeline_end():
    console.rule(f"[bold magenta]Pipeline Finished[/bold magenta]")

