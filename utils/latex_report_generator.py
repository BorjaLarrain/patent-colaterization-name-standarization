"""
LaTeX Report Generator for Entity Statistics
===========================================
Functions to generate LaTeX reports with graphs and compile them to PDF.
"""

import subprocess
import shutil
from pathlib import Path
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)

def generate_latex_report(graph_paths: Dict[str, Path], output_path: Path, 
                         title: str = "Entity Frequency Statistics Report") -> Path:
    """
    Generate a LaTeX source file with graphs included.
    
    Args:
        graph_paths: Dictionary mapping graph names to file paths. Expected keys:
                     - 'financial_security_bar', 'financial_security_percentage'
                     - 'financial_release_bar', 'financial_release_percentage'
                     - 'non_financial_security_bar', 'non_financial_security_percentage'
                     - 'non_financial_release_bar', 'non_financial_release_percentage'
                     - 'security_pair_bar', 'security_pair_percentage' (optional)
                     - 'release_pair_bar', 'release_pair_percentage' (optional)
        output_path: Path where to save the LaTeX file (.tex)
        title: Title of the report
        
    Returns:
        Path to the generated LaTeX file
    """
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert graph paths to relative paths from LaTeX file location
    latex_dir = output_path.parent.resolve()  # Use absolute path for comparison
    relative_graph_paths = {}
    for key, graph_path in graph_paths.items():
        graph_path_resolved = Path(graph_path).resolve()
        try:
            # Try to make path relative to LaTeX directory
            rel_path = graph_path_resolved.relative_to(latex_dir)
            relative_graph_paths[key] = str(rel_path).replace('\\', '/')  # Use forward slashes for LaTeX
        except ValueError:
            # If not relative (different roots), compute relative path manually
            # Find common parent and build relative path
            try:
                # Try to find common path components
                latex_parts = latex_dir.parts
                graph_parts = graph_path_resolved.parts
                
                # Find common prefix
                common_len = 0
                for i in range(min(len(latex_parts), len(graph_parts))):
                    if latex_parts[i] == graph_parts[i]:
                        common_len = i + 1
                    else:
                        break
                
                if common_len > 0:
                    # Build relative path: go up from latex_dir, then down to graph
                    up_levels = len(latex_parts) - common_len
                    down_path = graph_parts[common_len:]
                    rel_path = Path('../' * up_levels) / Path(*down_path)
                    relative_graph_paths[key] = str(rel_path).replace('\\', '/')
                else:
                    # No common path, use absolute path
                    relative_graph_paths[key] = str(graph_path_resolved).replace('\\', '/')
            except Exception:
                # Fallback to absolute path
                relative_graph_paths[key] = str(graph_path_resolved).replace('\\', '/')
    
    # LaTeX template
    latex_content = f"""\\documentclass[11pt]{{article}}
\\usepackage[utf8]{{inputenc}}
\\usepackage{{graphicx}}
\\usepackage[margin=1in]{{geometry}}
\\usepackage{{caption}}
\\usepackage{{subcaption}}
\\usepackage{{booktabs}}
\\usepackage{{float}}

\\title{{{title}}}
\\author{{Generated Report}}
\\date{{\\today}}

\\begin{{document}}

\\maketitle

\\tableofcontents
\\newpage

\\section{{Financial Security Entities}}

This section shows statistics for financial entities in security (pledge) transactions.

\\subsection{{Top 20 Entities by Frequency}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('financial_security_bar', '')}}}
    \\caption{{Top 20 financial security entities ranked by total frequency}}
\\end{{figure}}

\\subsection{{Top 20 Entities by Percentage of Total}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('financial_security_percentage', '')}}}
    \\caption{{Top 20 financial security entities showing percentage of total frequency}}
\\end{{figure}}

\\newpage

\\section{{Financial Release Entities}}

This section shows statistics for financial entities in release transactions.

\\subsection{{Top 20 Entities by Frequency}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('financial_release_bar', '')}}}
    \\caption{{Top 20 financial release entities ranked by total frequency}}
\\end{{figure}}

\\subsection{{Top 20 Entities by Percentage of Total}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('financial_release_percentage', '')}}}
    \\caption{{Top 20 financial release entities showing percentage of total frequency}}
\\end{{figure}}

\\newpage

\\section{{Non-Financial Security Entities}}

This section shows statistics for non-financial entities in security (pledge) transactions.

\\subsection{{Top 20 Entities by Frequency}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('non_financial_security_bar', '')}}}
    \\caption{{Top 20 non-financial security entities ranked by total frequency}}
\\end{{figure}}

\\subsection{{Top 20 Entities by Percentage of Total}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('non_financial_security_percentage', '')}}}
    \\caption{{Top 20 non-financial security entities showing percentage of total frequency}}
\\end{{figure}}

\\newpage

\\section{{Non-Financial Release Entities}}

This section shows statistics for non-financial entities in release transactions.

\\subsection{{Top 20 Entities by Frequency}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('non_financial_release_bar', '')}}}
    \\caption{{Top 20 non-financial release entities ranked by total frequency}}
\\end{{figure}}

\\subsection{{Top 20 Entities by Percentage of Total}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('non_financial_release_percentage', '')}}}
    \\caption{{Top 20 non-financial release entities showing percentage of total frequency}}
\\end{{figure}}

\\newpage

\\section{{Patent Transaction Pairs Analysis}}

This section shows statistics for (firm-bank) pairs in patent transactions.

\\subsection{{Security Transaction Pairs}}

This subsection shows the top 20 (firm-bank) pairs in security (pledge) transactions.

\\subsubsection{{Top 20 (Firm-Bank) Pairs by Frequency}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('security_pair_bar', '')}}}
    \\caption{{Top 20 (firm-bank) pairs in security transactions ranked by total frequency}}
\\end{{figure}}

\\subsubsection{{Top 20 (Firm-Bank) Pairs by Percentage of Total}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('security_pair_percentage', '')}}}
    \\caption{{Top 20 (firm-bank) pairs in security transactions showing percentage of total frequency}}
\\end{{figure}}

\\newpage

\\subsection{{Release Transaction Pairs}}

This subsection shows the top 20 (firm-bank) pairs in release transactions.

\\subsubsection{{Top 20 (Firm-Bank) Pairs by Frequency}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('release_pair_bar', '')}}}
    \\caption{{Top 20 (firm-bank) pairs in release transactions ranked by total frequency}}
\\end{{figure}}

\\subsubsection{{Top 20 (Firm-Bank) Pairs by Percentage of Total}}
\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=\\textwidth]{{{relative_graph_paths.get('release_pair_percentage', '')}}}
    \\caption{{Top 20 (firm-bank) pairs in release transactions showing percentage of total frequency}}
\\end{{figure}}

\\end{{document}}
"""
    
    # Write LaTeX file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(latex_content)
    
    logger.info(f"LaTeX file generated: {output_path}")
    return output_path


def compile_latex_to_pdf(latex_path: Path, output_dir: Optional[Path] = None, 
                        cleanup_aux: bool = True) -> Optional[Path]:
    """
    Compile LaTeX file to PDF using pdflatex.
    
    Args:
        latex_path: Path to the .tex file
        output_dir: Directory where to place the PDF (default: same as LaTeX file)
        cleanup_aux: Whether to clean up auxiliary files (.aux, .log, .out)
        
    Returns:
        Path to the generated PDF file, or None if compilation failed
    """
    if not latex_path.exists():
        logger.error(f"LaTeX file not found: {latex_path}")
        return None
    
    if output_dir is None:
        output_dir = latex_path.parent
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Ensure output directory is writable
    import os
    if not os.access(output_dir, os.W_OK):
        logger.error(f"Output directory is not writable: {output_dir}")
        return None
    
    # Change to LaTeX file directory for compilation (so relative image paths work)
    original_dir = Path.cwd()
    latex_dir = latex_path.parent
    latex_filename = latex_path.name
    
    # If output directory is same as LaTeX directory, simplify (no -output-directory needed)
    # Otherwise, use -output-directory with absolute path
    use_output_dir_flag = (output_dir.resolve() != latex_dir.resolve())
    
    try:
        # Change to LaTeX directory so relative paths in .tex file resolve correctly
        os.chdir(latex_dir)
        
        # Build pdflatex command
        if use_output_dir_flag:
            output_dir_abs = output_dir.resolve()
            cmd = ['pdflatex', '-interaction=nonstopmode', '-output-directory', str(output_dir_abs), latex_filename]
        else:
            # Output directory is same as LaTeX directory, so just compile normally
            cmd = ['pdflatex', '-interaction=nonstopmode', latex_filename]
        
        # Run pdflatex (twice for proper references)
        result1 = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        # Second compilation (for references)
        result2 = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        # Check if PDF was generated
        pdf_name = latex_path.stem + '.pdf'
        
        # Resolve output_dir to absolute path once
        output_dir_abs = output_dir.resolve()
        pdf_path = output_dir_abs / pdf_name
        
        # Also check in current directory (where pdflatex might have written it)
        # This handles the case where we're in latex_dir and pdflatex writes to current dir
        current_dir_pdf = Path.cwd() / pdf_name
        current_dir_pdf_resolved = current_dir_pdf.resolve()
        
        # Use whichever PDF exists (prefer output_dir, but check current dir too)
        if pdf_path.exists():
            final_pdf_path = pdf_path.resolve()
        elif current_dir_pdf.exists():
            # PDF was generated in current directory
            if not use_output_dir_flag:
                # output_dir == latex_dir, so PDF is already in the right place
                # Use the resolved path from current directory since it exists
                final_pdf_path = current_dir_pdf_resolved
            else:
                # output_dir != latex_dir, try to move PDF to output_dir
                final_pdf_path = current_dir_pdf_resolved
                if str(final_pdf_path) != str(pdf_path.resolve()):
                    try:
                        Path(final_pdf_path).rename(pdf_path.resolve())
                        final_pdf_path = pdf_path.resolve()
                    except Exception as e:
                        logger.warning(f"Could not move PDF from {final_pdf_path} to {pdf_path.resolve()}: {e}")
                        # Keep using current_dir_pdf_resolved if move fails
        else:
            final_pdf_path = None
            logger.warning(f"PDF not found in either location. pdf_path: {pdf_path}, current_dir_pdf: {current_dir_pdf}")
        
        if final_pdf_path and final_pdf_path.exists():
            logger.info(f"PDF successfully generated: {final_pdf_path}")
            
            # Check for actual errors (not just warnings about missing images)
            # LaTeX warnings are common and don't prevent PDF generation
            has_errors = False
            if result2.returncode != 0:
                has_errors = True
                logger.warning(f"LaTeX compilation had non-zero exit code: {result2.returncode}")
            
            # Log warnings about missing images if any (these are common and don't prevent PDF generation)
            if "not found" in result2.stdout or "not found" in result2.stderr:
                logger.warning("Some image files were not found. PDF was still generated but may be missing images.")
            
            # Clean up auxiliary files if requested
            if cleanup_aux:
                aux_extensions = ['.aux', '.log', '.out', '.toc']
                for ext in aux_extensions:
                    # Check both output_dir and current directory
                    aux_file = output_dir.resolve() / (latex_path.stem + ext)
                    if not aux_file.exists():
                        aux_file = Path.cwd() / (latex_path.stem + ext)
                    if aux_file.exists():
                        try:
                            aux_file.unlink()
                        except Exception as e:
                            logger.warning(f"Could not delete auxiliary file {aux_file}: {e}")
            
            return final_pdf_path
        else:
            # PDF was not generated - this is a real error
            logger.error(f"PDF compilation failed - no PDF file was created. LaTeX output:\n{result2.stdout}\n{result2.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error("LaTeX compilation timed out")
        return None
    except FileNotFoundError:
        logger.error("pdflatex not found. Please ensure LaTeX is installed (e.g., via brew install basictex or mactex)")
        return None
    except Exception as e:
        logger.error(f"Error during LaTeX compilation: {e}")
        return None
    finally:
        # Change back to original directory
        os.chdir(original_dir)

